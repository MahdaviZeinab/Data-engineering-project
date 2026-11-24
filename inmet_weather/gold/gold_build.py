"""
Gold layer: how likely is a fire in a state on a day with bad fire weather?

It joins the two Silver tables per state and local day:
  silver.inmet_hourly (weather, one row per station and hour)
  silver.fires        (clean fires, one row per fire focus)

Steps:
  1. weather: hours -> one row per station and day -> one row per state and day
  2. fires:   one row per fire focus -> number of fires per state and day
  3. join both per state and day, add days without rain and the risk score (0-6)
  4. report: per risk score, the share of days that had a fire (= fire probability)

Tables (database "gold"):
  gold.fire_weather_daily        one row per state and local day, split by year
  gold.fire_probability_by_score one row per state (and "ALL") and risk score

All rules and thresholds are in gold/gold_contract.py.

Run (from the inmet_weather folder):
  python -m gold.gold_build                  # all years, dev
  python -m gold.gold_build --years 2025     # only 2025
  python -m gold.gold_build --env prod       # production lake
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.checks import has_columns, not_empty, one_row_per, run_checks
from common.report import add_to_summary, markdown_table, report_failure
from common.spark import save_table, start_spark
from gold import gold_contract as rules

KEY = ["state", "local_date"]


# Step 0: read Silver
# ---------------------------------------------------------------------------
def read_silver(spark, years=None):
    # Read the two Silver tables. If years are given, keep only those years.
    # We keep only rows whose local day is in the same year as their file:
    # the first UTC hours of 1 January are still 31 December in Brazil, and
    # would otherwise put a few rows of the year before into this year's run.
    weather = spark.table("silver.inmet_hourly")
    fires = spark.table("silver.fires")
    if years:
        weather = weather.where(F.col("source_year").isin(years))
        fires = fires.where(F.col("source_year").isin(years))
    same_year = F.year("local_date") == F.col("source_year")
    return weather.where(same_year), fires.where(same_year)


# Step 1: weather per state and day
# ---------------------------------------------------------------------------
def station_days(weather):
    # One row per station and day. Rain is ADDED over the hours (it is a total),
    # the other values take the day's maximum or minimum.
    # A day with too few hours is dropped: its rain total would be too small.
    days = weather.groupBy("station_code", "state", "local_date", "source_year").agg(
        F.count("*").alias("hours"),
        F.max("temp_max_c").alias("temp_max_c"),
        F.min("humidity_min_pct").alias("humidity_min_pct"),
        F.sum("precip_mm").alias("rain_mm"),
        F.greatest(F.max("wind_speed_ms"), F.max("wind_gust_ms")).alias("wind_max_ms"),
    )
    return days.where(F.col("hours") >= rules.MIN_HOURS)


def state_weather(days):
    # One row per state and day: the average of the state's stations.
    return days.groupBy("state", "local_date", "source_year").agg(
        F.count("*").alias("stations"),
        F.round(F.avg("temp_max_c"), 2).alias("temp_max_c"),
        F.round(F.avg("humidity_min_pct"), 2).alias("humidity_min_pct"),
        F.round(F.avg("rain_mm"), 2).alias("rain_mm"),
        F.round(F.avg("wind_max_ms"), 2).alias("wind_max_ms"),
    )


# Step 2: fires per state and day
# ---------------------------------------------------------------------------
def state_fires(fires):
    # Number of fire foci per state and day. Rows with a quality problem are left out.
    clean = fires.where(F.col("dq_issue").isNull() & F.col("state").isNotNull())
    return clean.groupBy("state", "local_date", "source_year").agg(F.count("*").alias("fires"))


# Step 3: join, days without rain, risk score
# ---------------------------------------------------------------------------
def join_weather_and_fires(weather_days, fire_days):
    # Full join: a day with weather but no fire gets fires = 0,
    # a day with fires but no weather is kept with weather_ok = false.
    daily = weather_days.join(fire_days, ["state", "local_date", "source_year"], "full")
    daily = daily.fillna(0, ["fires", "stations"])
    daily = daily.withColumn("has_fire", F.col("fires") > 0)
    return daily.withColumn("weather_ok", F.col("stations") > 0)


def add_days_without_rain(daily):
    # Count the days since the state's last rain day (0 on a rain day).
    # Trick: every rain day starts a new group; inside a group we count the days.
    # Days without weather are not rain days, so the count goes on through them.
    by_date = Window.partitionBy("state").orderBy("local_date")
    rain_day = F.coalesce(F.col("rain_mm") >= rules.RAIN_DAY_MM, F.lit(False))
    daily = daily.withColumn("rain_group", F.sum(rain_day.cast("int")).over(
        by_date.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    in_group = Window.partitionBy("state", "rain_group").orderBy("local_date")
    daily = daily.withColumn("days_without_rain", F.row_number().over(in_group) - 1)
    return daily.drop("rain_group")


def points(condition, value=1):
    # value points when the condition is true, 0 when it is false or unknown.
    return F.when(condition, value).otherwise(0)


def add_risk_score(daily):
    # Add the bad-weather points (rules in gold_contract.py). Score 0 (no risk) to 6.
    # No weather that day -> no score (null), the day is left out of the report.
    score = (points(F.col("humidity_min_pct") < rules.HUMIDITY_LOW)
             + points(F.col("humidity_min_pct") < rules.HUMIDITY_VERY_LOW)
             + points(F.col("temp_max_c") > rules.TEMP_HIGH)
             + points(F.col("wind_max_ms") > rules.WIND_STRONG_MS)
             + points(F.col("days_without_rain") > rules.DRY_DAYS)
             + points(F.col("days_without_rain") > rules.DRY_DAYS_LONG))
    return daily.withColumn("risk_score", F.when(F.col("weather_ok"), score))


# Step 4: fire probability per risk score
# ---------------------------------------------------------------------------
def probability_by_score(daily):
    # For each state and risk score: how many days, how many had a fire,
    # and the share of days with a fire. "ALL" = all states together.
    days = daily.where(F.col("weather_ok"))
    measures = [
        F.count("*").alias("days"),
        F.sum(F.col("has_fire").cast("int")).alias("fire_days"),
        F.sum("fires").alias("fires"),
    ]
    per_state = days.groupBy("state", "risk_score").agg(*measures)
    country = days.groupBy("risk_score").agg(*measures).withColumn("state", F.lit(rules.ALL_STATES))
    report = per_state.unionByName(country)
    report = report.withColumn("fire_probability", F.round(F.col("fire_days") / F.col("days"), 3))
    report = report.withColumn("fires_per_day", F.round(F.col("fires") / F.col("days"), 2))
    return report.orderBy("state", "risk_score")


# The whole job
# ---------------------------------------------------------------------------
def run_gold(spark, years=None, run_id=None):
    # Do steps 1-3 and return the daily table (not saved yet).
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    weather, fires = read_silver(spark, years)
    daily = join_weather_and_fires(state_weather(station_days(weather)), state_fires(fires))
    daily = add_risk_score(add_days_without_rain(daily))
    return daily.withColumn("_gold_run_id", F.lit(run_id)).cache()   # checked, then saved


def check_fire_total(spark, daily, years=None):
    # Every clean fire in Silver must be counted once in Gold (no fire lost or doubled).
    _, fires = read_silver(spark, years)
    expected = fires.where(F.col("dq_issue").isNull() & F.col("state").isNotNull()).count()
    counted = daily.agg(F.sum("fires")).first()[0] or 0
    if counted != expected:
        raise ValueError(f"Gold counts {counted} fires, Silver has {expected}; nothing was saved")


def save_gold(spark, daily, years=None):
    # Check the daily table, save it (only the years we ran),
    # then rebuild the small report from ALL years in the saved table.
    run_checks(daily, [not_empty, has_columns(*KEY, "fires", "risk_score"), one_row_per(*KEY)])
    check_fire_total(spark, daily, years)
    save_table(spark, daily, rules.DAILY, years)
    report = probability_by_score(spark.table(rules.DAILY))
    save_table(spark, report, rules.PROBABILITY)


def print_summary(spark):
    # Print the country-wide result in the log: fire probability per risk score.
    print("GOLD: fire probability per risk score, all states")
    (spark.table(rules.PROBABILITY).where(F.col("state") == rules.ALL_STATES)
          .orderBy("risk_score").show())


def write_report(spark, years=None):
    # The Gold result per year on the GitHub run page (see common/report.py).
    daily = spark.table(rules.DAILY)
    if years:
        daily = daily.where(F.col("source_year").isin(years))
    rows = (daily.groupBy("source_year").agg(
                F.count("*").alias("state_days"),
                F.sum(F.col("weather_ok").cast("int")).alias("with_weather"),
                F.sum(F.col("has_fire").cast("int")).alias("with_fire"),
                F.sum("fires").alias("fires"))
            .orderBy("source_year").collect())
    add_to_summary("### Gold\n\n" + markdown_table(
        ["year", "state-days", "with weather", "with fire", "fires"], [tuple(r) for r in rows]))


def main():
    # Read the command line options, start Spark, build Gold and save it.
    parser = argparse.ArgumentParser(description="Build the Gold fire-risk tables from Silver.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("gold", lake, memory="2g")
    try:
        daily = run_gold(spark, args.years)
        save_gold(spark, daily, args.years)
        print(f"GOLD: saved {rules.DAILY} and {rules.PROBABILITY}")
        print_summary(spark)
        write_report(spark, args.years)        # the result on the GitHub run page
        return 0
    except Exception as error:
        print("GOLD FAILED:", error)
        report_failure("Gold", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
