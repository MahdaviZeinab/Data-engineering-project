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

from silver import contract


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


def cast_values(df):
    """Silver 4: rename with RENAME; MISSING_CODES -> null; ',' -> '.'; cast to double."""
    return df


def make_time(df):
    """Silver 5: Data + Hora UTC -> observed_at_utc; add timezone and local time."""
    return df


def remove_duplicates(df):
    """Silvre 6: keep one row per KEY."""
    return df


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
    df = cast_values(df)
    df = make_time(df)
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
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = run_silver(spark, output, args.years)
        print("SILVER rows:", silver.count())       # writing comes with S0/S10
        return 0
    except Exception as error:
        print("SILVER FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
