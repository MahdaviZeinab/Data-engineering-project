"""
Value checks for the INMET measurements.

  invalid  physically impossible (humidity 130 %)  -> value set to null
  unusual  possible but rare (120 mm rain in 1 h)  -> value kept

Both add a flag to dq_flags, e.g. ["invalid_humidity_pct", "unusual_precip_mm"].
Only the affected value changes, never the whole row. Limits are in inmet_contract.py.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver import inmet_contract as contract


def is_outside(column, low, high):
    # None = no limit; a null value never matches
    value = F.col(column)
    if low is None:
        return value > high
    if high is None:
        return value < low
    return (value < low) | (value > high)


def add_flags(df, conditions):
    # conditions: {flag name: condition}; appends the flags that are true
    new_flags = F.array(*[F.when(condition, F.lit(name)) for name, condition in conditions.items()])
    return df.withColumn("dq_flags", F.array_distinct(F.array_compact(F.concat("dq_flags", new_flags))))


def has_flag(name):
    return F.array_contains("dq_flags", name)


def invalid_conditions():
    # a column can have several rules, combined with OR
    conditions = {}

    def add(column, condition):
        old = conditions.get(column)
        conditions[column] = condition if old is None else (old | condition)

    # outside physical limits
    for column, (low, high) in contract.INVALID.items():
        add(column, is_outside(column, low, high))

    # max below min: unclear which one is wrong, so both are invalid
    for max_column, min_column in contract.INVALID_MAX_BELOW_MIN:
        max_below_min = F.col(max_column) < F.col(min_column)
        add(max_column, max_below_min)
        add(min_column, max_below_min)

    # dew point above air temperature
    add("dew_point_c", F.col("dew_point_c") > F.col("temp_c") + contract.DEW_POINT_TOLERANCE_C)

    return {f"invalid_{column}": condition for column, condition in conditions.items()}


def temp_jump():
    # big temperature change vs. the previous hour (only if exactly 1 h earlier)
    hour_before = Window.partitionBy("station_code").orderBy("observed_at_utc")
    temp_before = F.lag("temp_c").over(hour_before)
    time_before = F.lag("observed_at_utc").over(hour_before)
    one_hour_later = F.col("observed_at_utc").cast("long") - time_before.cast("long") == 3600
    big_change = F.abs(F.col("temp_c") - temp_before) > contract.UNUSUAL_TEMP_JUMP_C
    return one_hour_later & big_change


def unusual_conditions():
    conditions = {f"unusual_{column}": is_outside(column, low, high)
                  for column, (low, high) in contract.UNUSUAL.items()}
    conditions["unusual_temp_jump"] = temp_jump()
    return conditions


def apply_value_rules(df):
    df = df.withColumn("dq_flags", F.array().cast("array<string>"))

    # negative radiation at night -> 0 (flagged so it can be counted)
    if contract.NEGATIVE_RADIATION_TO_ZERO:
        negative = F.col("radiation_kj_m2") < 0
        df = add_flags(df, {"radiation_negative_set_to_0": negative})
        df = df.withColumn("radiation_kj_m2",
                           F.when(negative, F.lit(0.0)).otherwise(F.col("radiation_kj_m2")))

    # flag on the original values first, then null the flagged ones
    invalid = invalid_conditions()
    df = add_flags(df, invalid)
    for flag in invalid:
        column = flag.removeprefix("invalid_")
        df = df.withColumn(column, F.when(has_flag(flag), None).otherwise(F.col(column)))

    return add_flags(df, unusual_conditions())


def count_flags(df):
    # flags per year, for the log and the quality report
    return (df.select("source_year", F.explode("dq_flags").alias("flag"))
              .groupBy("source_year", "flag").count()
              .orderBy("source_year", "flag"))
