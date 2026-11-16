"""
Silver job: turn the raw Bronze tables into clean weather data.

All rules come from silver/contract.py. Each cleaning step is one
function below. For now the steps only pass the data throughthe
next issues fill them in, one step at a time.

"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.publish import has_columns, not_empty, publish
from silver import contract
from silver.stations import build_stations


# Read Bronze
# --------------------------------------------------------------------------
def read_bronze(spark, output, years=None):
    """Return (hourly, files) from <output>/bronze. All years unless --years is given."""
    hourly = spark.read.parquet(f"{output}/bronze/inmet_hourly")
    files = spark.read.parquet(f"{output}/bronze/inmet_files")
    if years:
        hourly = hourly.where(F.col("source_year").isin(years))
        files = files.where(F.col("source_year").isin(years))
    return hourly, files


# Cleaning steps (one function per step, filled in by later issues)
# --------------------------------------------------------------------------
def check_schema(df):
    """Silver 2: stop if Bronze has a missing or a new column."""
    contract.check_schema(df.columns)
    return df

#issue #7: cast values to double, with ',' -> '.' and MISSING_CODES -> null
def to_text(bronze_name):
    """Bronze text, trimmed, with ',' -> '.' (so "25,5" -> "25.5" and ",8" -> ".8")."""
    return F.regexp_replace(F.trim(F.col(f"`{bronze_name}`")), ",", ".")


def cast_values(df):
    """Silver 4: rename with RENAME; MISSING_CODES -> null; ',' -> '.'; cast to double."""
    for bronze_name, silver_name in contract.RENAME.items():
        text = to_text(bronze_name)
        number = F.when(text.isin(contract.MISSING_CODES), None).otherwise(text.cast("double"))
        df = df.withColumn(silver_name, number).drop(bronze_name)
    return df
#--------------------------------------------------------------------------


def count_not_numeric(df):
    """Silver 4: per measurement, how many Bronze values are text that is not a number."""
    counts = []
    for bronze_name, silver_name in contract.RENAME.items():
        text = to_text(bronze_name)
        bad = text.isNotNull() & ~text.isin(contract.MISSING_CODES) & text.cast("double").isNull()
        counts.append(F.sum(bad.cast("int")).alias(silver_name))
    return df.agg(*counts).first().asDict()


def station_timezones(files):
    """Silver 5: find the time zone of each station.
    First check the station list, if it's not there use the state's zone."""
    spark = files.sparkSession
    by_state = spark.createDataFrame(list(contract.STATE_TIMEZONE.items()), "state string, state_tz string")
    by_station = spark.createDataFrame(list(contract.STATION_TIMEZONE_OVERRIDE.items()),
                                       "station_code string, station_tz string")
    return (files.select("station_code", F.upper(F.trim("state")).alias("state"))
                 .dropDuplicates(["station_code"])
                 .join(by_state, "state", "left")
                 .join(by_station, "station_code", "left")
                 .select("station_code", F.coalesce("station_tz", "state_tz").alias("timezone")))


def make_time(df, files):
    """Silver 5: make the UTC time and the station's local time.
    Bad rows are kept and marked in dq_issue. Silver 9 moves them out later."""
    day = F.regexp_replace("Data", "-", "/")                  # same date format for all
    hhmm = F.regexp_replace("Hora UTC", "[^0-9]", "")         # keep only the digits, "0000"
    utc = F.when(hhmm.rlike("^([01][0-9]|2[0-3])[0-5][0-9]$"),   # hour must be 00-23
                 F.to_timestamp(F.concat_ws(" ", day, hhmm), "yyyy/MM/dd HHmm"))

    # add the time zone, then turn UTC into local time
    df = (df.withColumn("observed_at_utc", utc)
            .join(F.broadcast(station_timezones(files)), "station_code", "left")
            .withColumn("local_time", F.from_utc_timestamp("observed_at_utc", F.col("timezone")))
            .withColumn("local_date", F.to_date("local_time"))
            .withColumn("local_hour", F.hour("local_time")))
    # mark rows we can't use
    return df.withColumn("dq_issue",
                         F.when(F.col("observed_at_utc").isNull(), "bad_timestamp")
                          .when(F.col("timezone").isNull(), "unknown_timezone"))


def count_duplicates(df):
    """Silver 6: how many extra rows have the same KEY (station + hour). Like Lab 3."""
    rows = df.where(F.col("observed_at_utc").isNotNull()).select(contract.KEY)
    return rows.count() - rows.distinct().count()


def remove_duplicates(df):
    """Silver 6: keep one row per KEY (station + hour), the best one:
    1. date is in the right year (same as source_year)
    2. fewer missing values
    3. newest Bronze record
    Rows without observed_at_utc are kept; Silver 9 handles them."""
    right_year = (F.year("observed_at_utc") == F.col("source_year")).cast("int")
    missing = sum(F.col(c).isNull().cast("int") for c in contract.MEASUREMENTS)

    best_first = Window.partitionBy(*contract.KEY).orderBy(
        right_year.desc(), missing.asc(), F.col("_bronze_ingestion_time").desc())

    df = df.withColumn("row_num", F.row_number().over(best_first))
    keep = (F.col("row_num") == 1) | F.col("observed_at_utc").isNull()
    return df.where(keep).drop("row_num")


def apply_value_rules(df):
    """Silver 8: INVALID values -> null, UNUSUAL values -> flag."""
    return df


def quarantine_rows(df):
    """Silver 9: put rows we can't use aside, with a reason."""
    return df


def add_station(df, files):
    """Silver 10: add state, region, station name and timezone from the station tables."""
    return df


# --------------------------------------------------------------------------
# The whole job
# --------------------------------------------------------------------------
def run_silver(spark, output, years=None, run_id=None):
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    hourly, files = read_bronze(spark, output, years)

    df = check_schema(hourly)
    print("Values that are not numbers:", count_not_numeric(df))
    df = cast_values(df)
    df = make_time(df, files)
    print("Duplicates removed:", count_duplicates(df))
    df = remove_duplicates(df)
    df = apply_value_rules(df)
    df = quarantine_rows(df)
    df = add_station(df, files)
    return df.withColumn("_silver_run_id", F.lit(run_id))


def main():
    parser = argparse.ArgumentParser(description="Build the Silver tables from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # Same rule as Bronze: CI / the VM can choose the folder with DAT535_OUTPUT_DIR.
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    output = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = (SparkSession.builder.appName("inmet-silver")
             .config("spark.driver.memory", "2g")   # VM has 8 GB, shared with other programs
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        silver = run_silver(spark, output, args.years, run_id)
        table = f"{output}/silver/inmet_hourly"
        rows = publish(spark, silver, table, run_id, partition_by="source_year",
                       checks=[not_empty, has_columns(*contract.KEY, *contract.MEASUREMENTS)])

        # show the result in the log
        print("SILVER published to", table)
        print("SILVER rows per year:", rows)
        result = spark.read.parquet(table)
        result.select("station_code", "observed_at_utc", "local_time", "timezone",
                      "temp_c", "humidity_pct", "precip_mm", "dq_issue", "source_year") \
              .show(10, truncate=False)
        result.groupBy("dq_issue").count().show()

        # Silver 7: station tables (small, always built from all years)
        build_stations(spark, output, run_id)
        return 0
    except Exception as error:
        print("SILVER FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
