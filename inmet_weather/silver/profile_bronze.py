"""
S1: profile the Bronze data before we clean it in Silver.

This job only READS Bronze. It never changes, deletes or rewrites a Bronze table.

Run (from the inmet_weather folder):
  python -m silver.profile_bronze                  # all years, dev
  python -m silver.profile_bronze --years 2025     # only 2025
  python -m silver.profile_bronze --env prod       # production folder
"""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bronze.bronze_ingest import COLUMNS

# The 17 weather measurements = every Bronze column except "Data" and "Hora UTC".
MEASUREMENTS = COLUMNS[2:]
MISSING_CODE = -9999   # INMET writes -9999 for "no value"


# --------------------------------------------------------------------------
# Step 2: read the two Bronze tables
# --------------------------------------------------------------------------
def read_bronze(spark, output, years=None):
    """Return (hourly, files) from <output>/bronze, optionally only some years."""
    bronze = f"{output}/bronze"
    hourly = spark.read.parquet(f"{bronze}/inmet_hourly")
    files = spark.read.parquet(f"{bronze}/inmet_files")
    if years:
        # source_year is the partition column, so Spark only reads these folders
        hourly = hourly.where(F.col("source_year").isin(years))
        files = files.where(F.col("source_year").isin(years))
    return hourly, files


# --------------------------------------------------------------------------
# Step 3: profile the 17 measurements, per measurement and year
# --------------------------------------------------------------------------
def profile_measurements(hourly):
    """
    One row per (year, measurement) with counts and statistics.
    Bronze keeps every value as text, so we only make a TEMPORARY number here;
    the Bronze columns themselves are never changed.
    """
    # 1. Long format: one row per (year, measurement, raw text value).
    #    Then ONE groupBy handles all 17 measurements the same way.
    pairs = [F.struct(F.lit(name).alias("measurement"), F.col(f"`{name}`").alias("raw"))
             for name in MEASUREMENTS]
    values = hourly.select("source_year", F.explode(F.array(*pairs)).alias("m")) \
                   .select("source_year", "m.measurement", F.trim("m.raw").alias("raw"))

    # 2. Temporary number: "884,1" -> "884.1" -> 884.1 (try_cast gives null if it fails)
    number = F.expr("try_cast(regexp_replace(raw, ',', '.') AS DOUBLE)")
    # text like "NaN" also casts to a double, but it is not a real number
    values = values.withColumn("number", F.when(~F.isnan(number), number))

    # 3. Each value is exactly one of: empty, -9999, not a number, or valid.
    is_empty = F.col("raw").isNull() | (F.col("raw") == "")
    is_missing_code = F.col("number") == MISSING_CODE
    is_not_numeric = ~is_empty & F.col("number").isNull()
    valid = F.when(~is_empty & ~is_missing_code & F.col("number").isNotNull(), F.col("number"))

    def count_if(condition):
        return F.sum(F.when(condition, 1).otherwise(0))

    # 4. Statistics only on valid numbers (empty, -9999 and text are left out).
    return (values.groupBy("source_year", "measurement").agg(
                F.count("*").alias("total_values"),
                count_if(is_empty).alias("empty_values"),
                count_if(is_missing_code).alias("missing_code_values"),
                count_if(is_not_numeric).alias("not_numeric_values"),
                F.count(valid).alias("valid_values"),
                F.min(valid).alias("min"),
                F.percentile_approx(valid, [0.01, 0.5, 0.99]).alias("p"),
                F.max(valid).alias("max"))
            .select("source_year", "measurement", "total_values", "empty_values",
                    "missing_code_values", "not_numeric_values", "valid_values", "min",
                    F.col("p")[0].alias("p01"), F.col("p")[1].alias("median"),
                    F.col("p")[2].alias("p99"), "max")
            .orderBy("measurement", "source_year"))


# --------------------------------------------------------------------------
# Step 4: timestamps and missing hours (UTC only; local time is S5)
# --------------------------------------------------------------------------
# What a correct value looks like. Anything else counts as a bad date / hour.
DATE_PATTERN = r"^\d{4}[/-]\d{2}[/-]\d{2}$"                   # 2020/01/31 or 2020-01-31
HOUR_PATTERN = r"^([01]\d|2[0-3])(00 UTC|:00)$"              # 0000 UTC ... 2300 UTC, or 00:00


def add_utc_time(hourly):
    """Add TEMPORARY utc_date and utc_hour columns (null when the text is bad)."""
    date_text = F.regexp_replace("Data", "-", "/")
    utc_date = F.when(F.col("Data").rlike(DATE_PATTERN), F.to_date(date_text, "yyyy/MM/dd"))
    utc_hour = F.when(F.col("`Hora UTC`").rlike(HOUR_PATTERN),
                      F.substring("`Hora UTC`", 1, 2).cast("int"))
    return hourly.withColumn("utc_date", utc_date).withColumn("utc_hour", utc_hour)


def timestamp_formats(hourly):
    """
    Which text formats appear in "Data" and "Hora UTC"? Every digit becomes 9,
    so "2020/01/31" -> "9999/99/99" and "0100 UTC" -> "9999 UTC".
    """
    def formats(column):
        return hourly.select("source_year", F.lit(column).alias("column"),
                             F.regexp_replace(F.col(f"`{column}`"), r"\d", "9").alias("format"))
    return (formats("Data").unionByName(formats("Hora UTC"))
            .groupBy("source_year", "column", "format").count()
            .orderBy("column", "source_year", F.desc("count")))


def timestamp_problems(hourly):
    """Per year: how many rows have a date or hour we cannot read."""
    timed = add_utc_time(hourly)
    return (timed.groupBy("source_year").agg(
                F.count("*").alias("rows"),
                F.sum(F.when(F.col("utc_date").isNull(), 1).otherwise(0)).alias("bad_date"),
                F.sum(F.when(F.col("utc_hour").isNull(), 1).otherwise(0)).alias("bad_hour"))
            .orderBy("source_year"))


def hours_per_station_day(hourly, files):
    """
    One row per station and UTC day with the number of hours we have (0-24).
    - distinct hours, so a duplicate row does not count twice;
    - days with NO rows come from the file dates (start_date..end_date),
      because a missing day does not appear in inmet_hourly at all.
    """
    observed = (add_utc_time(hourly)
                .where(F.col("utc_date").isNotNull() & F.col("utc_hour").isNotNull())
                .groupBy("station_code", "utc_date")
                .agg(F.countDistinct("utc_hour").alias("hours")))

    expected = (files.where(F.col("start_date").isNotNull() & F.col("end_date").isNotNull())
                .select("station_code",
                        F.explode(F.sequence("start_date", "end_date")).alias("utc_date"))
                .distinct())

    # full join: a day with data outside the file dates is also kept
    return (expected.join(observed, ["station_code", "utc_date"], "full")
            .fillna(0, ["hours"])
            .withColumn("year", F.year("utc_date")))


def hours_histogram(station_days):
    """How many station-days have 24, 23, ..., 0 hours (per year)."""
    return (station_days.groupBy("year", "hours").count()
            .withColumnRenamed("count", "station_days")
            .orderBy("year", F.desc("hours")))


# --------------------------------------------------------------------------
# Step 5: station information (from inmet_files, one row per file)
# --------------------------------------------------------------------------
def to_number(column):
    """TEMPORARY number from Bronze text: "-15,78" -> -15.78 (null if not a number)."""
    return F.expr(f"try_cast(regexp_replace(trim({column}), ',', '.') AS DOUBLE)")


def stations_per_state(files):
    """How many different stations each state has (all years together)."""
    return (files.groupBy("state")
            .agg(F.countDistinct("station_code").alias("stations"))
            .orderBy(F.desc("stations")))


def stations_per_year(files):
    """How many different stations have a file in each year."""
    return (files.groupBy("source_year")
            .agg(F.countDistinct("station_code").alias("stations"))
            .orderBy("source_year"))


def station_changes(files):
    """
    One row per station whose name, state, latitude or longitude is not
    the same in every year. Nothing is corrected here; we only look.
    """
    per_station = files.groupBy("station_code").agg(
        F.sort_array(F.collect_set("source_year")).alias("years"),
        F.collect_set(F.trim("station_name")).alias("names"),
        F.collect_set(F.trim("state")).alias("states"),
        # how far the station "moved" between years, in degrees
        F.round(F.max(to_number("latitude")) - F.min(to_number("latitude")), 6).alias("lat_change"),
        F.round(F.max(to_number("longitude")) - F.min(to_number("longitude")), 6).alias("lon_change"))

    return (per_station
            .withColumn("name_changed", F.size("names") > 1)
            .withColumn("state_changed", F.size("states") > 1)
            .withColumn("position_changed", (F.col("lat_change") > 0) | (F.col("lon_change") > 0))
            .where("name_changed OR state_changed OR position_changed")
            .orderBy("station_code"))


def main():
    parser = argparse.ArgumentParser(description="Profile the Bronze tables (read only).")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # Same rule as Bronze: CI / the VM can choose the folder with DAT535_OUTPUT_DIR.
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    output = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = (SparkSession.builder.appName("inmet-profile-bronze")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        hourly, files = read_bronze(spark, output, args.years)
        # Quick check that both tables were read: rows per year.
        print("inmet_hourly rows per year:")
        hourly.groupBy("source_year").count().orderBy("source_year").show()
        print("inmet_files rows per year:")
        files.groupBy("source_year").count().orderBy("source_year").show()

        print("Step 3: measurements per year:")
        profile_measurements(hourly).show(200, truncate=False)

        print("Step 4: timestamp formats:")
        timestamp_formats(hourly).show(100, truncate=False)
        print("Step 4: rows with a bad date or hour:")
        timestamp_problems(hourly).show()
        print("Step 4: station-days by number of hours (24 = complete, 0 = no data):")
        station_days = hours_per_station_day(hourly, files)
        hours_histogram(station_days).show(200)

        print("Step 5: stations per state:")
        stations_per_state(files).show(50)
        print("Step 5: stations per year:")
        stations_per_year(files).show()
        changes = station_changes(files)
        print("Step 5: stations whose name, state or position changed:", changes.count())
        changes.show(20, truncate=False)
        return 0
    except Exception as error:
        print("PROFILE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
