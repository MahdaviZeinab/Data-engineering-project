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
        return 0
    except Exception as error:
        print("PROFILE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
