"""
Silver 8: invalid values and unusual values.

Two kinds of problems:
  invalid = physically impossible (humidity 130 %)  -> the value becomes empty (null)
  unusual = possible, but rare (120 mm rain in 1 h) -> the value stays

Both get a flag in the new column dq_flags (a list of text), for example
["invalid_humidity_pct", "unusual_precip_mm"]. Only that one value changes,
the rest of the row stays. All limits are in silver/contract.py.

The steps, in this order:
  1. small negative radiation (sensor noise at night) -> 0
  2. invalid values -> flag, then empty
  3. unusual values -> flag only
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver import contract


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def is_outside(column, low, high):
    # True if the value is below low or above high. None means "no limit".
    # An empty value gives null, and null never gets a flag.
    value = F.col(column)
    if low is None:
        return value > high
    if high is None:
        return value < low
    return (value < low) | (value > high)


def add_flags(df, conditions):
    # conditions = {flag name: condition}. Add every flag whose condition is true
    # to the list in dq_flags. (F.when gives null when false; array_compact removes nulls.)
    new_flags = F.array(*[F.when(condition, F.lit(name)) for name, condition in conditions.items()])
    return df.withColumn("dq_flags", F.array_distinct(F.array_compact(F.concat("dq_flags", new_flags))))


def has_flag(name):
    return F.array_contains("dq_flags", name)


# ---------------------------------------------------------------------------
# The rules (numbers come from contract.py)
# ---------------------------------------------------------------------------
def invalid_conditions():
    # For every column: when is its value impossible?
    # A column can have more than one rule (humidity_max_pct: 0-100 AND max >= min).
    conditions = {}

    def add(column, condition):
        old = conditions.get(column)
        conditions[column] = condition if old is None else (old | condition)

    # a) outside the physical limits, for example humidity above 100 %
    for column, (low, high) in contract.INVALID.items():
        add(column, is_outside(column, low, high))

    # b) max below min in the same hour: we don't know which one is wrong, so both become empty
    for max_column, min_column in contract.INVALID_MAX_BELOW_MIN:
        max_below_min = F.col(max_column) < F.col(min_column)
        add(max_column, max_below_min)
        add(min_column, max_below_min)

    # c) dew point above air temperature (+ a little room for sensor error)
    add("dew_point_c", F.col("dew_point_c") > F.col("temp_c") + contract.DEW_POINT_TOLERANCE_C)

    return {f"invalid_{column}": condition for column, condition in conditions.items()}


def temp_jump():
    # The temperature changed more than UNUSUAL_TEMP_JUMP_C since the hour before,
    # at the same station. Only compared when the row before is exactly 1 hour earlier.
    hour_before = Window.partitionBy("station_code").orderBy("observed_at_utc")
    temp_before = F.lag("temp_c").over(hour_before)
    time_before = F.lag("observed_at_utc").over(hour_before)
    one_hour_later = F.col("observed_at_utc").cast("long") - time_before.cast("long") == 3600
    big_change = F.abs(F.col("temp_c") - temp_before) > contract.UNUSUAL_TEMP_JUMP_C
    return one_hour_later & big_change


def unusual_conditions():
    # Possible, but rare: keep the value, only add a flag.
    conditions = {f"unusual_{column}": is_outside(column, low, high)
                  for column, (low, high) in contract.UNUSUAL.items()}
    conditions["unusual_temp_jump"] = temp_jump()
    return conditions


# ---------------------------------------------------------------------------
# The step that silver_build.py runs
# ---------------------------------------------------------------------------
def apply_value_rules(df):
    # Start every row with an empty list of flags.
    df = df.withColumn("dq_flags", F.array().cast("array<string>"))

    # 1. small negative radiation at night is sensor noise -> 0 (and counted with a flag)
    if contract.NEGATIVE_RADIATION_TO_ZERO:
        negative = F.col("radiation_kj_m2") < 0
        df = add_flags(df, {"radiation_negative_set_to_0": negative})
        df = df.withColumn("radiation_kj_m2",
                           F.when(negative, F.lit(0.0)).otherwise(F.col("radiation_kj_m2")))

    # 2. invalid: first add all flags (on the original values), then empty the flagged values
    invalid = invalid_conditions()
    df = add_flags(df, invalid)
    for flag in invalid:
        column = flag.removeprefix("invalid_")
        df = df.withColumn(column, F.when(has_flag(flag), None).otherwise(F.col(column)))

    # 3. unusual: only a flag, the value stays
    return add_flags(df, unusual_conditions())


def count_flags(df):
    # How many values got each flag, per year. Used in the run log and the quality report (S11).
    return (df.select("source_year", F.explode("dq_flags").alias("flag"))
              .groupBy("source_year", "flag").count()
              .orderBy("source_year", "flag"))
