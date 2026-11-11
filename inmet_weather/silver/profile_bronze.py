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
        return 0
    except Exception as error:
        print("PROFILE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
