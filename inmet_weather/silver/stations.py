"""
Silver 7: station information.

Bronze keeps the 8 info lines of every INMET file in bronze/inmet_files
(one row per file = one station in one year). From it we make two tables:

  silver/station_history   one row per station per year, as the file said it,
                           with name_changed / location_changed vs the year before
  silver/station           one row per station: the latest information,
                           plus first/last year and how many changes it had

The table is small (about 600 stations x years), so we always read ALL years,
also when the hourly job runs with --years. Otherwise the history would lose years.

Run on its own:
  python -m silver.stations              # dev
  python -m silver.stations --env prod
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.publish import has_columns, not_empty, publish
from common.spark import start_spark
from silver import contract


# Small helpers
# --------------------------------------------------------------------------
def to_number(name):
    """'-15,78944444' -> -15.78944444 (INMET writes ',' as the decimal sign)."""
    text = F.regexp_replace(F.trim(F.col(name)), ",", ".")
    return F.when(text.isin(contract.MISSING_CODES), None).otherwise(text.cast("double"))


def to_date(name):
    """INMET writes the foundation date as 2000-05-07, 07/05/2000 or 07/05/00."""
    text = F.trim(F.col(name))
    return (F.when(text.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(text, "yyyy-MM-dd"))
             .when(text.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(text, "dd/MM/yyyy"))
             .when(text.rlike(r"^\d{2}/\d{2}/\d{2}$"), F.to_date(text, "dd/MM/yy")))


def lookup(mapping, key):
    """Find a column's value in a Python dict, e.g. state -> time zone."""
    if not mapping:
        return F.lit(None).cast("string")
    pairs = [F.lit(x) for item in mapping.items() for x in item]
    return F.element_at(F.create_map(*pairs), key)


# Step 1: one row per station per year
# --------------------------------------------------------------------------
def station_history(files):
    """Clean the station info of every file. One row per station and year."""
    # a bad file name has no station code, so we can't use that file
    df = files.where(F.col("station_code").isNotNull() & (F.col("station_code") != ""))

    # rare: two files for one station in one year -> keep the one that ends last
    newest_file = Window.partitionBy("station_code", "source_year") \
                        .orderBy(F.col("end_date").desc(), F.col("source_file").desc())
    df = df.withColumn("n", F.row_number().over(newest_file)).where("n = 1")

    state = F.upper(F.trim("state"))
    df = df.select(
        "station_code",
        "source_year",
        F.upper(F.trim("region")).alias("region"),
        state.alias("state"),
        F.trim("station_name").alias("station_name"),
        F.trim("wmo_code").alias("wmo_code"),
        to_number("latitude").alias("latitude"),
        to_number("longitude").alias("longitude"),
        to_number("altitude").alias("altitude"),
        to_date("foundation_date").alias("foundation_date"),
        # same rule as Silver 5: the station's own zone first, else its state's zone
        F.coalesce(lookup(contract.STATION_TIMEZONE_OVERRIDE, F.col("station_code")),
                   lookup(contract.STATE_TIMEZONE, state)).alias("timezone"),
        "source_file",
        "_bronze_run_id",
    )
    return add_checks(df)


# Step 2: checks (inside Brazil? changed since last year?)
# --------------------------------------------------------------------------
def add_checks(df):
    lat_min, lat_max = contract.BRAZIL_BOX["lat"]
    lon_min, lon_max = contract.BRAZIL_BOX["lon"]
    inside = F.col("latitude").between(lat_min, lat_max) & F.col("longitude").between(lon_min, lon_max)

    # compare each year with the year before, for the same station
    before = Window.partitionBy("station_code").orderBy("source_year")

    def moved(column, tolerance):
        return F.abs(F.col(column) - F.lag(column).over(before)) > tolerance

    new_name = F.upper(F.col("station_name")) != F.upper(F.lag("station_name").over(before))
    new_place = (moved("latitude", contract.LOCATION_TOLERANCE_DEG)
                 | moved("longitude", contract.LOCATION_TOLERANCE_DEG)
                 | moved("altitude", contract.ALTITUDE_TOLERANCE_M))

    # first year (nothing before) or a missing value -> null -> False
    return (df.withColumn("in_brazil", F.coalesce(inside, F.lit(False)))
              .withColumn("name_changed", F.coalesce(new_name, F.lit(False)))
              .withColumn("location_changed", F.coalesce(new_place, F.lit(False))))


# Step 3: one row per station, the latest information
# --------------------------------------------------------------------------
def latest_station(history):
    station = Window.partitionBy("station_code")
    newest_first = station.orderBy(F.col("source_year").desc())
    return (history
            .withColumn("first_year", F.min("source_year").over(station))
            .withColumn("years", F.count("*").over(station))
            .withColumn("name_changes", F.sum(F.col("name_changed").cast("int")).over(station))
            .withColumn("location_changes", F.sum(F.col("location_changed").cast("int")).over(station))
            .withColumn("n", F.row_number().over(newest_first))
            .where("n = 1")
            .withColumnRenamed("source_year", "last_year")
            .drop("n", "name_changed", "location_changed"))


# The whole step: read Bronze, build, publish
# --------------------------------------------------------------------------
def build_stations(spark, output, run_id):
    files = spark.table("bronze.inmet_files")      # all years, on purpose
    history = station_history(files).withColumn("_silver_run_id", F.lit(run_id)).cache()
    latest = latest_station(history)

    key_history = has_columns("station_code", "source_year", "latitude", "longitude", "timezone")
    key_latest = has_columns("station_code", "last_year", "latitude", "longitude", "timezone")
    publish(spark, history, f"{output}/silver/station_history", run_id,
            checks=[not_empty, key_history, one_row_per("station_code", "source_year")])
    publish(spark, latest, f"{output}/silver/station", run_id,
            checks=[not_empty, key_latest, one_row_per("station_code")])

    # short report for the log
    print("STATIONS:", latest.count(), "| station-years:", history.count())
    print("Stations outside Brazil (or no coordinates):",
          latest.where(~F.col("in_brazil")).count())
    print("Stations without time zone:", latest.where(F.col("timezone").isNull()).count())
    changes = history.where(F.col("name_changed") | F.col("location_changed"))
    print("Name or location changes between years:", changes.count())
    changes.select("station_code", "source_year", "station_name", "latitude", "longitude",
                   "altitude", "name_changed", "location_changed") \
           .orderBy("station_code", "source_year").show(20, truncate=False)
    history.unpersist()


def one_row_per(*key):
    """A publish check: the key must be unique."""
    def check(df):
        extra = df.count() - df.select(*key).distinct().count()
        if extra:
            raise ValueError(f"{extra} duplicate rows for key {list(key)}")
    return check


def main():
    parser = argparse.ArgumentParser(description="Build the Silver station tables from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    output = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-silver-stations", output, memory="2g")
    try:
        build_stations(spark, output, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        return 0
    except Exception as error:
        print("SILVER STATIONS FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
