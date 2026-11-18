"""
Silver 1: look at the Bronze data before we clean it.

This job only READS Bronze; it never changes a Bronze table.
Every result is a small Spark table in the database "profile",
for example profile.measurements. Look at one with:
    spark.table("profile.measurements").show()

Run (from the inmet_weather folder):
  python -m silver.profile_bronze                  # all years, dev
  python -m silver.profile_bronze --years 2025     # only 2025
  python -m silver.profile_bronze --env prod       # production lake
"""

import argparse
import os
import sys
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F

from bronze.bronze_ingest import COLUMNS
from common.spark import save_table, start_spark

MEASUREMENTS = COLUMNS[2:]   # the 17 measurements = every column except data and hora_utc
MISSING_CODE = -9999         # INMET writes -9999 for "no value"

DATE_PATTERN = r"^\d{4}[/-]\d{2}[/-]\d{2}$"         # 2020/01/31 or 2020-01-31
HOUR_PATTERN = r"^([01]\d|2[0-3])(00 UTC|:00)$"    # 0000 UTC ... 2300 UTC, or 00:00


# ---------------------------------------------------------------------------
# Read Bronze
# ---------------------------------------------------------------------------
def read_bronze(spark, years=None):
    # Read the two Bronze tables. If years are given, keep only those years.
    hourly = spark.table("bronze.inmet_hourly")
    files = spark.table("bronze.inmet_files")
    if years:
        hourly = hourly.where(F.col("source_year").isin(years))
        files = files.where(F.col("source_year").isin(years))
    return hourly, files


def to_number(column):
    # Make a number from Bronze text, only to look at it: "884,1" -> 884.1.
    # Text that is not a number gives empty (null). Bronze itself is not changed.
    return F.expr(f"try_cast(regexp_replace(trim(`{column}`), ',', '.') AS DOUBLE)")


# ---------------------------------------------------------------------------
# The 17 measurements
# ---------------------------------------------------------------------------
def profile_one_measurement(hourly, name):
    # For one measurement and every year: how many values are empty, -9999,
    # not a number, or valid, and the min / median / max of the valid ones.
    raw = F.trim(F.col(name))
    number = to_number(name)
    is_empty = raw.isNull() | (raw == "")
    is_missing_code = number == MISSING_CODE
    is_text = ~is_empty & number.isNull()
    valid = F.when(~is_empty & ~is_missing_code & number.isNotNull(), number)

    return hourly.groupBy("source_year").agg(
        F.lit(name).alias("measurement"),
        F.count("*").alias("total_values"),
        F.sum(is_empty.cast("int")).alias("empty_values"),
        F.sum(F.coalesce(is_missing_code, F.lit(False)).cast("int")).alias("missing_code_values"),
        F.sum(is_text.cast("int")).alias("not_numeric_values"),
        F.count(valid).alias("valid_values"),
        F.min(valid).alias("min"),
        F.percentile_approx(valid, 0.01).alias("p01"),
        F.percentile_approx(valid, 0.5).alias("median"),
        F.percentile_approx(valid, 0.99).alias("p99"),
        F.max(valid).alias("max"),
    )


def profile_measurements(hourly):
    # Profile all 17 measurements and put the results under each other in one table.
    tables = [profile_one_measurement(hourly, name) for name in MEASUREMENTS]
    return reduce(lambda a, b: a.unionByName(b), tables)


# ---------------------------------------------------------------------------
# Dates, hours and missing hours (UTC)
# ---------------------------------------------------------------------------
def add_utc_time(hourly):
    # Add utc_date and utc_hour, only to look at them.
    # They are empty when the text is not a good date or hour.
    date_text = F.regexp_replace("data", "-", "/")
    utc_date = F.when(F.col("data").rlike(DATE_PATTERN), F.to_date(date_text, "yyyy/MM/dd"))
    utc_hour = F.when(F.col("hora_utc").rlike(HOUR_PATTERN),
                      F.substring("hora_utc", 1, 2).cast("int"))
    return hourly.withColumn("utc_date", utc_date).withColumn("utc_hour", utc_hour)


def timestamp_formats(hourly):
    # Which ways of writing the date and the hour are used? Every digit becomes 9,
    # so "2020/01/31" -> "9999/99/99" and "0100 UTC" -> "9999 UTC".
    dates = hourly.select("source_year", F.lit("data").alias("column"),
                          F.regexp_replace("data", r"\d", "9").alias("format"))
    hours = hourly.select("source_year", F.lit("hora_utc").alias("column"),
                          F.regexp_replace("hora_utc", r"\d", "9").alias("format"))
    return dates.unionByName(hours).groupBy("source_year", "column", "format").count()


def timestamp_problems(hourly):
    # Per year: how many rows have a date or an hour we can not read.
    timed = add_utc_time(hourly)
    return timed.groupBy("source_year").agg(
        F.count("*").alias("rows"),
        F.sum(F.col("utc_date").isNull().cast("int")).alias("bad_date"),
        F.sum(F.col("utc_hour").isNull().cast("int")).alias("bad_hour"))


def hours_per_station_day(hourly, files):
    # For every station and day: how many different hours we have (0 to 24).
    # Days with no rows at all come from the file dates (start_date to end_date),
    # because a missing day is not in the hourly table.
    observed = (add_utc_time(hourly)
                .where(F.col("utc_date").isNotNull() & F.col("utc_hour").isNotNull())
                .groupBy("station_code", "utc_date")
                .agg(F.countDistinct("utc_hour").alias("hours")))
    expected = (files.where(F.col("start_date").isNotNull() & F.col("end_date").isNotNull())
                .select("station_code",
                        F.explode(F.sequence("start_date", "end_date")).alias("utc_date"))
                .distinct())
    days = expected.join(observed, ["station_code", "utc_date"], "full").fillna(0, ["hours"])
    return days.withColumn("year", F.year("utc_date"))


def hours_histogram(station_days):
    # Per year: how many station-days have 24 hours, 23 hours, ... 0 hours.
    return (station_days.groupBy("year", "hours").count()
            .withColumnRenamed("count", "station_days"))


# ---------------------------------------------------------------------------
# Stations
# ---------------------------------------------------------------------------
def stations_per_state(files):
    # How many different stations each state has.
    return files.groupBy("state").agg(F.countDistinct("station_code").alias("stations"))


def stations_per_year(files):
    # How many different stations have a file in each year.
    return files.groupBy("source_year").agg(F.countDistinct("station_code").alias("stations"))


def station_changes(files):
    # Stations whose name, state or position is not the same in every year.
    # We only look here, nothing is corrected.
    per_station = files.groupBy("station_code").agg(
        F.concat_ws(",", F.sort_array(F.collect_set("source_year"))).alias("years"),
        F.concat_ws(" | ", F.collect_set(F.trim("station_name"))).alias("names"),
        F.countDistinct(F.trim("station_name")).alias("name_count"),
        F.countDistinct(F.trim("state")).alias("state_count"),
        (F.max(to_number("latitude")) - F.min(to_number("latitude"))).alias("lat_change"),
        (F.max(to_number("longitude")) - F.min(to_number("longitude"))).alias("lon_change"))
    changed = ((F.col("name_count") > 1) | (F.col("state_count") > 1)
               | (F.col("lat_change") > 0) | (F.col("lon_change") > 0))
    return per_station.where(changed)


# ---------------------------------------------------------------------------
# The whole profile job
# ---------------------------------------------------------------------------
def run_profile(spark, hourly, files):
    # Make every profile table, save it as a Spark table and print it.
    rows_per_year = (hourly.groupBy("source_year").agg(F.count("*").alias("hourly_rows"))
                     .join(files.groupBy("source_year").agg(F.count("*").alias("files")),
                           "source_year", "full"))
    station_days = hours_per_station_day(hourly, files)

    results = {
        "bronze_rows_per_year": rows_per_year,
        "measurements": profile_measurements(hourly),
        "timestamp_formats": timestamp_formats(hourly),
        "timestamp_problems": timestamp_problems(hourly),
        "hours_histogram": hours_histogram(station_days),
        "stations_per_state": stations_per_state(files),
        "stations_per_year": stations_per_year(files),
        "station_changes": station_changes(files),
    }
    for name, table in results.items():
        save_table(spark, table, f"profile.{name}")
        saved = spark.table(f"profile.{name}")
        print(f"\n== profile.{name}  ({saved.count()} rows)")
        saved.show(30, truncate=False)


def main():
    # Read the command line options, start Spark and make the profile.
    parser = argparse.ArgumentParser(description="Profile the Bronze tables (read only).")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-profile-bronze", lake)
    try:
        hourly, files = read_bronze(spark, args.years)
        run_profile(spark, hourly, files)
        return 0
    except Exception as error:
        print("PROFILE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
