"""
Silver layer for the fire data: turn bronze.fire_foci into clean fire foci.

Steps:
  1. read bronze.fire_foci and check it has the columns we expect
  2. clean the text and turn it into numbers and a time ("  -3.650000 " -> -3.65)
  3. add the state code (UF) and the local time, so fires can be joined
     with the weather data (same UF codes, same time zones)
  4. mark rows with a problem in dq_issue, also a point outside its state
     (silver/state_check.py)
  5. remove double rows (same foco_id)
  6. move rows with a problem to silver.fire_quarantine
  7. add the nearest weather station (silver/nearest_station.py)
  8. merge points of the same fire into one fire event (silver/fire_events.py)
  9. save silver.fires and silver.fire_quarantine together (split by year)

Run silver.stations first: step 7 reads silver.station_history.

All rules are in silver/fire_contract.py (fire only) and silver/contract.py (shared).

Run (from the inmet_weather folder):
  python -m silver.fire_build                   # all years, dev
  python -m silver.fire_build --years 2025      # only 2025
  python -m silver.fire_build --env prod        # production lake
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from bronze.fire_ingest import FIRE_TABLES
from bronze.bronze_ingest import check_tables_match
from common.checks import has_columns, not_empty, one_row_per, run_checks
from common.spark import move_all_from_staging, save_to_staging, start_spark
from silver import contract, fire_contract
from silver.fire_events import add_fire_events
from silver.nearest_station import add_nearest_station, mark_no_station
from silver.state_check import inside_state

TABLE = "silver.fires"
QUARANTINE = "silver.fire_quarantine"


# Step 1: read Bronze and check it
# ---------------------------------------------------------------------------
def read_bronze(spark, years=None):
    # Read bronze.fire_foci. If years are given, keep only those years.
    foci = spark.table("bronze.fire_foci")
    if years:
        foci = foci.where(F.col("source_year").isin(years))
    contract.check_schema(foci.columns, fire_contract.BRONZE_COLUMNS)
    return foci


# Step 2: text -> numbers and time
# ---------------------------------------------------------------------------
def text(column):
    # The Bronze text without the extra spaces. An empty text becomes null.
    value = F.trim(F.col(column))
    return F.when(value != "", value)


def cast_values(df):
    # Give the columns English names and the right types.
    return df.select(
        text("foco_id").alias("foco_id"),
        text("id_bdq").cast("long").alias("id_bdq"),
        F.to_timestamp(text("data_pas"), fire_contract.TIME_FORMAT).alias("observed_at_utc"),
        text("lat").cast("double").alias("lat"),
        text("lon").cast("double").alias("lon"),
        F.upper(text("estado")).alias("state_name"),
        text("municipio").alias("municipality"),
        text("bioma").alias("biome"),
        "source_year", "source_file", "source_line_number",
        "_bronze_run_id", "_bronze_ingestion_time",
    )


# Step 3: state code (UF) and local time
# ---------------------------------------------------------------------------
def states_table(spark):
    # A small table: full state name -> UF code -> time zone.
    # It has 27 rows, so Spark can send it to every worker (broadcast join).
    rows = [(name, uf, contract.STATE_TIMEZONE[uf])
            for name, uf in fire_contract.STATE_TO_UF.items()]
    return spark.createDataFrame(rows, "state_name string, state string, timezone string")


def add_state_and_time(spark, df):
    # Add the UF code (the same code the weather data uses) and the local time.
    # Fires are seen in the early afternoon, so the local day is the day of the fire.
    df = df.join(F.broadcast(states_table(spark)), "state_name", "left")
    df = df.withColumn("local_time", F.from_utc_timestamp("observed_at_utc", F.col("timezone")))
    df = df.withColumn("local_date", F.to_date("local_time"))
    return df.withColumn("local_hour", F.hour("local_time"))


# Step 4: problems
# ---------------------------------------------------------------------------
def inside_brazil():
    # True when lat and lon are inside the box around Brazil (same box as the stations).
    box = contract.BRAZIL_BOX
    return (F.col("lat").between(*box["lat"]) & F.col("lon").between(*box["lon"]))


def mark_problems(df):
    # dq_issue = the first problem of the row, or null when the row is fine.
    # Rows with a problem go to silver.fire_quarantine (step 6).
    problem = (F.when(F.col("foco_id").isNull(), "missing_id")
                .when(F.col("observed_at_utc").isNull(), "bad_timestamp")
                .when(F.col("lat").isNull() | F.col("lon").isNull(), "bad_coordinates")
                .when(~inside_brazil(), "outside_brazil")
                .when(F.col("state").isNull(), "unknown_state")
                .when(~inside_state(), "wrong_state")
                .when(~F.col("biome").isin(fire_contract.BIOMES), "unknown_biome"))
    return df.withColumn("dq_issue", problem)


# Step 5: double rows
# ---------------------------------------------------------------------------
def count_duplicates(df):
    # Count the extra rows: rows with the same foco_id as another row.
    ids = df.where(F.col("foco_id").isNotNull()).select(fire_contract.KEY)
    return ids.count() - ids.distinct().count()


def remove_duplicates(df):
    # Keep one row per foco_id: the one loaded last, then the first line in the file
    # (so two runs always keep the same row). Rows without an id are all kept.
    newest_first = (Window.partitionBy(*fire_contract.KEY)
                    .orderBy(F.col("_bronze_ingestion_time").desc(),
                             F.col("source_file").asc(), F.col("source_line_number").asc()))
    df = df.withColumn("rank", F.row_number().over(newest_first))
    keep = (F.col("rank") == 1) | F.col("foco_id").isNull()
    return df.where(keep).drop("rank", "_bronze_ingestion_time")


# Step 6: quarantine
# ---------------------------------------------------------------------------
def split_quarantine(df):
    # Good rows (no problem) and quarantine rows (with a problem in dq_issue).
    # Nothing is thrown away: every row ends up in one of the two tables.
    return df.where(F.col("dq_issue").isNull()), df.where(F.col("dq_issue").isNotNull())


# The whole job
# ---------------------------------------------------------------------------
def run_fire_silver(spark, years=None, run_id=None, stations=None):
    # Do all the steps and return (fires, quarantine), not saved yet.
    # stations: the station table to use (tests give a small one).
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df = cast_values(read_bronze(spark, years))
    df = add_state_and_time(spark, df)
    # cache(): Spark redoes every step for each count, check or save,
    # so we keep the tables we use more than once (spark.stop() frees them).
    df = mark_problems(df).cache()             # counted twice, then cleaned
    print("FIRE SILVER: double rows removed:", count_duplicates(df))
    df = remove_duplicates(df)
    fires, quarantine = split_quarantine(df)
    if stations is None:
        stations = spark.table("silver.station_history")
    fires = mark_no_station(add_nearest_station(fires, stations))
    fires = add_fire_events(fires)
    run = F.lit(run_id)
    # cache(): both are checked, then saved
    return (fires.withColumn("_silver_run_id", run).cache(),
            quarantine.withColumn("_silver_run_id", run).cache())


def save_fire_silver(spark, fires, quarantine, years=None):
    # Check the tables, then save both together: first to "<name>_new",
    # then into the real tables (if one fails, both get their old rows back).
    # Only the years we ran are replaced.
    run_checks(fires, [not_empty, has_columns("foco_id", "observed_at_utc", "state", "local_date"),
                       one_row_per(*fire_contract.KEY)])
    save_to_staging(spark, fires, TABLE)
    save_to_staging(spark, quarantine, QUARANTINE)
    move_all_from_staging(spark, {TABLE: years, QUARANTINE: years})


def print_summary(spark, years=None):
    # Print a short report in the log: rows per year, per problem, and a few rows.
    result = spark.table(TABLE)
    if years:
        result = result.where(F.col("source_year").isin(years))
    result.groupBy("source_year").count().orderBy("source_year").show()
    result.groupBy("dq_issue").count().show()
    print("FIRE SILVER: fire points:", result.count(),
          "| fire events:", result.where("first_in_event").count())
    result.select("foco_id", "observed_at_utc", "local_date", "state", "municipality",
                  "biome", "nearest_station_code", "distance_km", "fire_event_id",
                  "points_in_event").show(5, truncate=False)
    quarantine = spark.table(QUARANTINE)
    if years:
        quarantine = quarantine.where(F.col("source_year").isin(years))
    print(f"FIRE SILVER: rows in {QUARANTINE}, by problem:")
    quarantine.groupBy("dq_issue").count().show()


def main():
    # Read the command line options, start Spark, clean the data and save it.
    parser = argparse.ArgumentParser(description="Build silver.fires from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("fire-silver", lake, memory="2g")
    try:
        check_tables_match(spark, args.years, FIRE_TABLES)  # stop after a killed Bronze run
        fires, quarantine = run_fire_silver(spark, args.years)
        save_fire_silver(spark, fires, quarantine, args.years)
        print(f"FIRE SILVER: saved {TABLE} and {QUARANTINE}")
        print_summary(spark, args.years)
        return 0
    except Exception as error:
        print("FIRE SILVER FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
