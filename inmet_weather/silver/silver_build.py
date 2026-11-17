"""
Silver layer: turn the raw Bronze data into clean weather data.

Steps:
  1. read the Bronze tables
  2. check that Bronze has the columns we expect
  3. turn the text values into numbers ("25,5" -> 25.5, "-9999" -> empty)
  4. make the time: UTC time and the station's local time
  5. remove double rows (same station and same hour)
  6. save the result as the Spark table silver.inmet_hourly
  7. make the station tables (see stations.py)

All rules (names, missing codes, time zones ...) are in silver/contract.py.

Run (from the inmet_weather folder):
  python -m silver.silver_build                   # all years, dev
  python -m silver.silver_build --years 2025      # only 2025
  python -m silver.silver_build --env prod        # production lake
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.checks import has_columns, not_empty, run_checks
from common.spark import save_table, start_spark
from silver import contract
from silver.stations import build_stations, station_timezones


# ---------------------------------------------------------------------------
# Step 1 and 2: read Bronze and check it
# ---------------------------------------------------------------------------
def read_bronze(spark, years=None):
    # Read the two Bronze tables we need. If years are given, keep only those years.
    hourly = spark.table("bronze.inmet_hourly")
    files = spark.table("bronze.inmet_files")
    if years:
        hourly = hourly.where(F.col("source_year").isin(years))
        files = files.where(F.col("source_year").isin(years))
    return hourly, files


def check_schema(df):
    # Stop the job if Bronze has a missing or a new column,
    # so we never clean data we do not understand.
    contract.check_schema(df.columns)
    return df


# ---------------------------------------------------------------------------
# Step 3: text -> numbers
# ---------------------------------------------------------------------------
def clean_text(column):
    # Take a Bronze value and make it ready to become a number:
    # remove spaces and use "." instead of "," ("25,5" -> "25.5", ",8" -> ".8").
    return F.regexp_replace(F.trim(F.col(column)), ",", ".")


def cast_values(df):
    # Turn every measurement into a number with an English name,
    # for example temperatura_ar_c "25,5" -> temp_c 25.5.
    # INMET's "no value" codes ("" and "-9999") become empty (null).
    for bronze_name, silver_name in contract.RENAME.items():
        text = clean_text(bronze_name)
        is_missing = text.isin(contract.MISSING_CODES)
        number = F.when(is_missing, None).otherwise(text.cast("double"))
        df = df.withColumn(silver_name, number).drop(bronze_name)
    return df


def count_not_numeric(df):
    # Count, per measurement, the values that are not a number and not a
    # "no value" code (for example "abc"). They become empty, so we report them.
    counts = []
    for bronze_name, silver_name in contract.RENAME.items():
        text = clean_text(bronze_name)
        is_missing = text.isin(contract.MISSING_CODES)
        not_a_number = text.isNotNull() & ~is_missing & text.cast("double").isNull()
        counts.append(F.sum(not_a_number.cast("int")).alias(silver_name))
    return df.agg(*counts).first().asDict()


# ---------------------------------------------------------------------------
# Step 4: time
# ---------------------------------------------------------------------------
def utc_time():
    # Make one UTC time from the date and the hour text,
    # for example "2025/01/01" + "0300 UTC" -> 2025-01-01 03:00.
    # A wrong hour like "2500 UTC" gives an empty time.
    day = F.regexp_replace("data", "-", "/")             # 2025-01-01 -> 2025/01/01
    hhmm = F.regexp_replace("hora_utc", "[^0-9]", "")    # "0300 UTC" -> "0300"
    good_hour = hhmm.rlike("^([01][0-9]|2[0-3])[0-5][0-9]$")
    return F.when(good_hour, F.to_timestamp(F.concat_ws(" ", day, hhmm), "yyyy/MM/dd HHmm"))


def make_time(df, files):
    # Add the UTC time and the local time of the station.
    # We need local time, so rain in the evening is on the right local day.
    # Rows with a bad time or an unknown time zone are kept, but marked in dq_issue.
    df = df.withColumn("observed_at_utc", utc_time())
    df = df.join(F.broadcast(station_timezones(files)), "station_code", "left")
    df = df.withColumn("local_time", F.from_utc_timestamp("observed_at_utc", F.col("timezone")))
    df = df.withColumn("local_date", F.to_date("local_time"))
    df = df.withColumn("local_hour", F.hour("local_time"))
    problem = (F.when(F.col("observed_at_utc").isNull(), "bad_timestamp")
                .when(F.col("timezone").isNull(), "unknown_timezone"))
    return df.withColumn("dq_issue", problem)


# ---------------------------------------------------------------------------
# Step 5: double rows
# ---------------------------------------------------------------------------
def count_duplicates(df):
    # Count the extra rows: rows with the same station and hour as another row.
    rows = df.where(F.col("observed_at_utc").isNotNull()).select(contract.KEY)
    return rows.count() - rows.distinct().count()


def remove_duplicates(df):
    # Keep only one row per station and hour. We keep the best row:
    #   1. the date is in the same year as the file it came from
    #   2. it has fewer empty values
    #   3. it was loaded last
    # Rows without a time are all kept (we can not compare them).
    right_year = (F.year("observed_at_utc") == F.col("source_year")).cast("int")
    empty_values = sum(F.col(c).isNull().cast("int") for c in contract.MEASUREMENTS)
    best_first = (Window.partitionBy(*contract.KEY)
                  .orderBy(right_year.desc(), empty_values.asc(),
                           F.col("_bronze_ingestion_time").desc()))

    df = df.withColumn("rank", F.row_number().over(best_first))
    keep = (F.col("rank") == 1) | F.col("observed_at_utc").isNull()
    return df.where(keep).drop("rank")


# ---------------------------------------------------------------------------
# Next steps (empty for now, filled in by the next issues)
# ---------------------------------------------------------------------------
def apply_value_rules(df):
    # TODO Silver 8: impossible values -> empty, rare values -> flag (see contract.py).
    return df


def quarantine_rows(df):
    # TODO Silver 9: put rows we can not use aside, with a reason.
    return df


def add_station(df, files):
    # TODO Silver 10: add state, region and station name from the station tables.
    return df


# ---------------------------------------------------------------------------
# The whole Silver job
# ---------------------------------------------------------------------------
def run_silver(spark, years=None, run_id=None):
    # Do all the cleaning steps, one after the other, and return the clean table.
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    hourly, files = read_bronze(spark, years)

    df = check_schema(hourly)
    print("SILVER: values that are not numbers:", count_not_numeric(df))
    df = cast_values(df)
    df = make_time(df, files)
    print("SILVER: double rows removed:", count_duplicates(df))
    df = remove_duplicates(df)
    df = apply_value_rules(df)
    df = quarantine_rows(df)
    df = add_station(df, files)
    return df.withColumn("_silver_run_id", F.lit(run_id))


def print_summary(spark):
    # Print a short report in the log: rows per year and a few example rows.
    result = spark.table("silver.inmet_hourly")
    result.groupBy("source_year").count().orderBy("source_year").show()
    result.select("station_code", "observed_at_utc", "local_time", "timezone",
                  "temp_c", "humidity_pct", "precip_mm", "dq_issue", "source_year") \
          .show(10, truncate=False)
    result.groupBy("dq_issue").count().show()


def main():
    # Read the command line options, start Spark, clean the data and save it.
    parser = argparse.ArgumentParser(description="Build the Silver tables from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # The lake folder keeps the Spark tables. CI can choose it with DAT535_OUTPUT_DIR.
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-silver", lake, memory="2g")   # the VM has only 8 GB
    try:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        silver = run_silver(spark, args.years, run_id)

        # check the table, then save it (only the years we ran are replaced)
        run_checks(silver, [not_empty, has_columns(*contract.KEY, *contract.MEASUREMENTS)])
        save_table(spark, silver, "silver.inmet_hourly", args.years)
        print("SILVER: saved the Spark table silver.inmet_hourly")
        print_summary(spark)

        # the station tables are small, so they are always made from all years
        build_stations(spark, run_id)
        return 0
    except Exception as error:
        print("SILVER FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
