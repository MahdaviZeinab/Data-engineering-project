"""
Silver 7: the station tables.

Every INMET file starts with 8 lines about the station (name, state, latitude ...).
Bronze keeps them in bronze.inmet_files (one row per file = one station, one year).
From that table we make two Silver tables:

  silver.station_history   one row per station per year, with
                           name_changed / location_changed compared to the year before
  silver.station           one row per station: the newest information,
                           plus first year, last year and how many changes it had

These tables are small, so we always use ALL years.

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

from common.checks import has_columns, not_empty, one_row_per, run_checks
from common.spark import save_table, start_spark
from silver import contract


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def to_number(column):
    # Turn INMET text into a number: "-15,789" -> -15.789.
    # The "no value" codes ("" and "-9999") become empty (null).
    text = F.regexp_replace(F.trim(F.col(column)), ",", ".")
    return F.when(text.isin(contract.MISSING_CODES), None).otherwise(text.cast("double"))


def to_date(column):
    # Turn the foundation date into a real date.
    # INMET writes it in 3 ways: 2000-05-07, 07/05/2000 or 07/05/00.
    text = F.trim(F.col(column))
    return (F.when(text.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(text, "yyyy-MM-dd"))
             .when(text.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(text, "dd/MM/yyyy"))
             .when(text.rlike(r"^\d{2}/\d{2}/\d{2}$"), F.to_date(text, "dd/MM/yy")))


def station_timezones(files):
    # Find the time zone of every station. Brazil has 4 time zones.
    # First look in the station list (STATION_TIMEZONE_OVERRIDE),
    # if the station is not there, use the time zone of its state.
    spark = files.sparkSession
    by_state = spark.createDataFrame(list(contract.STATE_TIMEZONE.items()),
                                     "state string, state_tz string")
    by_station = spark.createDataFrame(list(contract.STATION_TIMEZONE_OVERRIDE.items()),
                                       "station_code string, station_tz string")
    stations = files.select("station_code", F.upper(F.trim("state")).alias("state"))
    stations = stations.dropDuplicates(["station_code"])
    stations = stations.join(by_state, "state", "left").join(by_station, "station_code", "left")
    return stations.select("station_code",
                           F.coalesce("station_tz", "state_tz").alias("timezone"))


# ---------------------------------------------------------------------------
# Step 1: one row per station per year
# ---------------------------------------------------------------------------
def keep_one_file_per_year(files):
    # Skip files without a station code (bad file name).
    # If one station has two files in one year (rare), keep the file that ends last.
    files = files.where(F.col("station_code").isNotNull() & (F.col("station_code") != ""))
    newest_file = (Window.partitionBy("station_code", "source_year")
                   .orderBy(F.col("end_date").desc(), F.col("source_file").desc()))
    files = files.withColumn("rank", F.row_number().over(newest_file))
    return files.where(F.col("rank") == 1).drop("rank")


def station_history(files):
    # Clean the station info of every file: trim the text, make numbers and dates,
    # and add the time zone. Then add the checks (see add_checks).
    files = keep_one_file_per_year(files)
    history = files.select(
        "station_code",
        "source_year",
        F.upper(F.trim("region")).alias("region"),
        F.upper(F.trim("state")).alias("state"),
        F.trim("station_name").alias("station_name"),
        F.trim("wmo_code").alias("wmo_code"),
        to_number("latitude").alias("latitude"),
        to_number("longitude").alias("longitude"),
        to_number("altitude").alias("altitude"),
        to_date("foundation_date").alias("foundation_date"),
        "source_file",
        "_bronze_run_id",
    )
    history = history.join(station_timezones(files), "station_code", "left")
    return add_checks(history)


# ---------------------------------------------------------------------------
# Step 2: checks
# ---------------------------------------------------------------------------
def add_checks(history):
    # Add 3 yes/no columns:
    #   in_brazil:        the coordinates are inside a box around Brazil
    #   name_changed:     the name is different from the year before
    #   location_changed: the station moved more than a little since the year before
    # The first year (nothing to compare with) or an empty value gives "no".
    lat_min, lat_max = contract.BRAZIL_BOX["lat"]
    lon_min, lon_max = contract.BRAZIL_BOX["lon"]
    inside = (F.col("latitude").between(lat_min, lat_max)
              & F.col("longitude").between(lon_min, lon_max))

    year_before = Window.partitionBy("station_code").orderBy("source_year")

    def moved(column, tolerance):
        # True when the value changed more than the tolerance since the year before
        return F.abs(F.col(column) - F.lag(column).over(year_before)) > tolerance

    new_name = F.upper(F.col("station_name")) != F.upper(F.lag("station_name").over(year_before))
    new_place = (moved("latitude", contract.LOCATION_TOLERANCE_DEG)
                 | moved("longitude", contract.LOCATION_TOLERANCE_DEG)
                 | moved("altitude", contract.ALTITUDE_TOLERANCE_M))

    history = history.withColumn("in_brazil", F.coalesce(inside, F.lit(False)))
    history = history.withColumn("name_changed", F.coalesce(new_name, F.lit(False)))
    return history.withColumn("location_changed", F.coalesce(new_place, F.lit(False)))


# ---------------------------------------------------------------------------
# Step 3: one row per station
# ---------------------------------------------------------------------------
def latest_station(history):
    # Keep the newest year of every station, and add a short summary:
    # first year, number of years, and how many name and location changes.
    station = Window.partitionBy("station_code")
    newest_first = station.orderBy(F.col("source_year").desc())

    df = history.withColumn("first_year", F.min("source_year").over(station))
    df = df.withColumn("years", F.count("*").over(station))
    df = df.withColumn("name_changes", F.sum(F.col("name_changed").cast("int")).over(station))
    df = df.withColumn("location_changes",
                       F.sum(F.col("location_changed").cast("int")).over(station))
    df = df.withColumn("rank", F.row_number().over(newest_first))
    df = df.where(F.col("rank") == 1).withColumnRenamed("source_year", "last_year")
    return df.drop("rank", "name_changed", "location_changed")


# ---------------------------------------------------------------------------
# The whole station step
# ---------------------------------------------------------------------------
def build_stations(spark, run_id):
    # Read Bronze, make the two station tables, check them and save them.
    files = spark.table("bronze.inmet_files")      # all years, on purpose
    history = station_history(files).withColumn("_silver_run_id", F.lit(run_id)).cache()
    latest = latest_station(history)

    run_checks(history, [not_empty,
                         has_columns("station_code", "source_year", "latitude", "longitude", "timezone"),
                         one_row_per("station_code", "source_year")])
    run_checks(latest, [not_empty,
                        has_columns("station_code", "last_year", "latitude", "longitude", "timezone"),
                        one_row_per("station_code")])
    save_table(spark, history, "silver.station_history")      # the whole table is replaced
    save_table(spark, latest, "silver.station")

    print_station_summary(history, latest)
    history.unpersist()


def print_station_summary(history, latest):
    # Print a short report in the log: how many stations, and which ones look strange.
    print("STATIONS:", latest.count(), "| station-years:", history.count())
    print("Stations outside Brazil (or no coordinates):", latest.where(~F.col("in_brazil")).count())
    print("Stations without time zone:", latest.where(F.col("timezone").isNull()).count())
    changes = history.where(F.col("name_changed") | F.col("location_changed"))
    print("Name or location changes between years:", changes.count())
    changes.select("station_code", "source_year", "station_name", "latitude", "longitude",
                   "altitude", "name_changed", "location_changed") \
           .orderBy("station_code", "source_year").show(20, truncate=False)


def main():
    # Run only the station step: start Spark, build the tables, stop Spark.
    parser = argparse.ArgumentParser(description="Build the Silver station tables from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-silver-stations", lake, memory="2g")
    try:
        build_stations(spark, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        return 0
    except Exception as error:
        print("SILVER STATIONS FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
