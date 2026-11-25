"""
Station tables, built from the 8 header lines of each INMET file (bronze.inmet_files).

  silver.station_history   one row per station-year, with name/location change flags
  silver.station           one row per station: latest info, first/last year, change counts

Small tables, so they are always built from all years.

Run on its own:
  python -m silver.inmet_stations              # dev
  python -m silver.inmet_stations --env prod
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.checks import has_columns, not_empty, one_row_per, run_checks
from bronze.bronze_ingest import check_tables_match
from common.spark import move_all_from_staging, save_to_staging, start_spark
from silver import inmet_contract as contract


# helpers
def to_number(column):
    # "-15,789" -> -15.789; missing codes -> null
    text = F.regexp_replace(F.trim(F.col(column)), ",", ".")
    return F.when(text.isin(contract.MISSING_CODES), None).otherwise(text.cast("double"))


def to_date(column):
    # foundation date comes as 2000-05-07, 07/05/2000 or 07/05/00
    text = F.trim(F.col(column))
    return (F.when(text.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(text, "yyyy-MM-dd"))
             .when(text.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(text, "dd/MM/yyyy"))
             .when(text.rlike(r"^\d{2}/\d{2}/\d{2}$"), F.to_date(text, "dd/MM/yy")))


def station_timezones(files):
    # station override first, otherwise the state's time zone
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


# station history
def keep_one_file_per_year(files):
    # skip files without a station code; if a station has two files in a year,
    # keep the one that ends last
    files = files.where(F.col("station_code").isNotNull() & (F.col("station_code") != ""))
    newest_file = (Window.partitionBy("station_code", "source_year")
                   .orderBy(F.col("end_date").desc(), F.col("source_file").desc()))
    files = files.withColumn("rank", F.row_number().over(newest_file))
    return files.where(F.col("rank") == 1).drop("rank")


def station_history(files):
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


def add_checks(history):
    # in_brazil, name_changed and location_changed (vs. the previous year).
    # First year or missing values give False.
    lat_min, lat_max = contract.BRAZIL_BOX["lat"]
    lon_min, lon_max = contract.BRAZIL_BOX["lon"]
    inside = (F.col("latitude").between(lat_min, lat_max)
              & F.col("longitude").between(lon_min, lon_max))

    year_before = Window.partitionBy("station_code").orderBy("source_year")

    def moved(column, tolerance):
        return F.abs(F.col(column) - F.lag(column).over(year_before)) > tolerance

    new_name = F.upper(F.col("station_name")) != F.upper(F.lag("station_name").over(year_before))
    new_place = (moved("latitude", contract.LOCATION_TOLERANCE_DEG)
                 | moved("longitude", contract.LOCATION_TOLERANCE_DEG)
                 | moved("altitude", contract.ALTITUDE_TOLERANCE_M))

    history = history.withColumn("in_brazil", F.coalesce(inside, F.lit(False)))
    history = history.withColumn("name_changed", F.coalesce(new_name, F.lit(False)))
    return history.withColumn("location_changed", F.coalesce(new_place, F.lit(False)))


# latest station
def latest_station(history):
    # newest year per station, plus first year, number of years and change counts
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


# build and save
STATION_TABLES = ["silver.station_history", "silver.station"]


def stage_stations(spark, run_id):
    # build, check and write both tables to "<name>_new"; live tables are untouched
    files = spark.table("bronze.inmet_files")      # all years on purpose
    history = station_history(files).withColumn("_silver_run_id", F.lit(run_id)).cache()
    latest = latest_station(history)

    run_checks(history, [not_empty,
                         has_columns("station_code", "source_year", "latitude", "longitude", "timezone"),
                         one_row_per("station_code", "source_year")])
    run_checks(latest, [not_empty,
                        has_columns("station_code", "last_year", "latitude", "longitude", "timezone"),
                        one_row_per("station_code")])
    save_to_staging(spark, history, "silver.station_history")
    save_to_staging(spark, latest, "silver.station")

    print_station_summary(history, latest)
    history.unpersist()


def build_stations(spark, run_id):
    # stage both tables, then replace them together
    stage_stations(spark, run_id)
    move_all_from_staging(spark, STATION_TABLES)


def print_station_summary(history, latest):
    print("STATIONS:", latest.count(), "| station-years:", history.count())
    print("Stations outside Brazil (or no coordinates):", latest.where(~F.col("in_brazil")).count())
    print("Stations without time zone:", latest.where(F.col("timezone").isNull()).count())
    changes = history.where(F.col("name_changed") | F.col("location_changed"))
    print("Name or location changes between years:", changes.count())
    changes.select("station_code", "source_year", "station_name", "latitude", "longitude",
                   "altitude", "name_changed", "location_changed") \
           .orderBy("station_code", "source_year").show(20, truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Build the Silver station tables from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-silver-stations", lake, memory="2g")
    try:
        check_tables_match(spark)          # Bronze must not be half replaced
        build_stations(spark, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        return 0
    except Exception as error:
        print("SILVER STATIONS FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
