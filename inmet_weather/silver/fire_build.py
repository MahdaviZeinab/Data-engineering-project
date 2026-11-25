"""
Silver job for the INPE fire data: bronze.fire_foci -> silver.fires.

Steps:
  1. read Bronze and check its columns
  2. text -> numbers and timestamp
  3. add state code (UF) and local time, to match the weather data
  4. mark problem rows in dq_issue (incl. fire_state_check.py)
  5. remove duplicate foco_ids
  6. move problem rows to silver.fire_quarantine
  7. add the nearest weather station (fire_station_matching.py)
  8. group nearby points into fire events (fire_events.py)
  9. save silver.fires and silver.fire_quarantine together

Needs silver.station_history, so run silver.inmet_stations first.

Run from the inmet_weather folder:
  python -m silver.fire_build                   # all years, dev
  python -m silver.fire_build --years 2025
  python -m silver.fire_build --env prod
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
from common.report import (add_to_summary, check_invalid_share, markdown_table, percent,
                           report_failure, show_tables)
from common.spark import move_all_from_staging, save_to_staging, start_spark
from silver import fire_contract, inmet_contract as contract
from silver.fire_events import add_fire_events
from silver.fire_station_matching import add_nearest_station, mark_no_station
from silver.quality import reconcile, rows_per_year
from silver.fire_state_check import inside_state

TABLE = "silver.fires"
QUARANTINE = "silver.fire_quarantine"


# read Bronze
def read_bronze(spark, years=None):
    foci = spark.table("bronze.fire_foci")
    if years:
        foci = foci.where(F.col("source_year").isin(years))
    contract.check_schema(foci.columns, fire_contract.BRONZE_COLUMNS)
    return foci


# text -> numbers and time
def text(column):
    # trimmed text, empty -> null
    value = F.trim(F.col(column))
    return F.when(value != "", value)


def cast_values(df):
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


# state code and local time
def states_table(spark):
    # state name -> UF -> time zone (27 rows, broadcast)
    rows = [(name, uf, contract.STATE_TIMEZONE[uf])
            for name, uf in fire_contract.STATE_TO_UF.items()]
    return spark.createDataFrame(rows, "state_name string, state string, timezone string")


def add_state_and_time(spark, df):
    df = df.join(F.broadcast(states_table(spark)), "state_name", "left")
    df = df.withColumn("local_time", F.from_utc_timestamp("observed_at_utc", F.col("timezone")))
    df = df.withColumn("local_date", F.to_date("local_time"))
    return df.withColumn("local_hour", F.hour("local_time"))


# problems
def inside_brazil():
    # same box as used for the stations
    box = contract.BRAZIL_BOX
    return (F.col("lat").between(*box["lat"]) & F.col("lon").between(*box["lon"]))


def mark_problems(df):
    # dq_issue = first problem found, null if the row is fine
    problem = (F.when(F.col("foco_id").isNull(), "missing_id")
                .when(F.col("observed_at_utc").isNull(), "bad_timestamp")
                .when(F.col("lat").isNull() | F.col("lon").isNull(), "bad_coordinates")
                .when(~inside_brazil(), "outside_brazil")
                .when(F.col("state").isNull(), "unknown_state")
                .when(~inside_state(), "wrong_state")
                .when(~F.col("biome").isin(fire_contract.BIOMES), "unknown_biome"))
    return df.withColumn("dq_issue", problem)


# duplicates
def count_duplicates(df):
    ids = df.where(F.col("foco_id").isNotNull()).select(fire_contract.KEY)
    return ids.count() - ids.distinct().count()


def remove_duplicates(df):
    # one row per foco_id: latest load, then first line in the file (stable across reruns).
    # Rows without an id are kept.
    newest_first = (Window.partitionBy(*fire_contract.KEY)
                    .orderBy(F.col("_bronze_ingestion_time").desc(),
                             F.col("source_file").asc(), F.col("source_line_number").asc()))
    df = df.withColumn("rank", F.row_number().over(newest_first))
    keep = (F.col("rank") == 1) | F.col("foco_id").isNull()
    return df.where(keep).drop("rank", "_bronze_ingestion_time")


# quarantine
def split_quarantine(df):
    # returns (good rows, quarantine rows)
    return df.where(F.col("dq_issue").isNull()), df.where(F.col("dq_issue").isNotNull())


# job
def run_fire_silver(spark, years=None, run_id=None, stations=None):
    # Returns (fires, quarantine), not saved yet. Tests can pass a small `stations` table.
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df = cast_values(read_bronze(spark, years))
    df = add_state_and_time(spark, df)
    df = mark_problems(df).cache()             # used for two counts
    print("FIRE SILVER: double rows removed:", count_duplicates(df))
    rows_in = rows_per_year(df)
    df = remove_duplicates(df)
    rows_kept = rows_per_year(df)
    duplicates = {year: rows_in[year] - rows_kept.get(year, 0) for year in rows_in}
    fires, quarantine = split_quarantine(df)
    if stations is None:
        stations = spark.table("silver.station_history")
    fires = mark_no_station(add_nearest_station(fires, stations))
    fires = add_fire_events(fires)
    run = F.lit(run_id)
    fires = fires.withColumn("_silver_run_id", run).cache()
    quarantine = quarantine.withColumn("_silver_run_id", run).cache()
    # stops here (before saving) if Bronze rows != fires + quarantine + duplicates
    counts = reconcile(rows_in, rows_per_year(fires), rows_per_year(quarantine), duplicates)
    print("FIRE SILVER: reconcile ok:", counts)
    return fires, quarantine


def save_fire_silver(spark, fires, quarantine, years=None):
    # check, write to "<name>_new", then swap both tables together.
    # Only the given years are replaced.
    run_checks(fires, [not_empty, has_columns("foco_id", "observed_at_utc", "state", "local_date"),
                       one_row_per(*fire_contract.KEY)])
    save_to_staging(spark, fires, TABLE)
    save_to_staging(spark, quarantine, QUARANTINE)
    move_all_from_staging(spark, {TABLE: years, QUARANTINE: years})


def print_summary(spark, years=None):
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


def saved_rows_per_year(spark, table, years=None):
    df = spark.table(table)
    if years:
        df = df.where(F.col("source_year").isin(years))
    return {int(r["source_year"]): r["count"] for r in df.groupBy("source_year").count().collect()}


def write_report(spark, years=None):
    # Results table for the GitHub run page.
    # invalid % = quarantined / Bronze rows; duplicates = Bronze - good - quarantined.
    bronze = saved_rows_per_year(spark, "bronze.fire_foci", years)
    good = saved_rows_per_year(spark, TABLE, years)
    bad = saved_rows_per_year(spark, QUARANTINE, years)
    rows = []
    for year in sorted(bronze):
        duplicates = bronze[year] - good.get(year, 0) - bad.get(year, 0)
        invalid_pct = percent(bad.get(year, 0), bronze[year])
        status = check_invalid_share("Silver fire", TABLE, year, invalid_pct)
        rows.append((year, bronze[year], good.get(year, 0), bad.get(year, 0), duplicates,
                     invalid_pct, status))
    text = "### Silver fire\n\n" + markdown_table(
        ["year", "rows from Bronze", "good rows", "quarantined", "duplicates removed",
         "invalid %", "status"], rows)

    quarantine = spark.table(QUARANTINE)
    if years:
        quarantine = quarantine.where(F.col("source_year").isin(years))
    reasons = (quarantine.groupBy("source_year", "dq_issue").count()
               .orderBy("source_year", F.desc("count")).collect())
    if reasons:
        text += "\n\nQuarantined rows by reason:\n\n" + markdown_table(
            ["year", "reason", "rows"], [tuple(r) for r in reasons])
    add_to_summary(text)


def main():
    parser = argparse.ArgumentParser(description="Build silver.fires from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("fire-silver", lake, memory="2g")
    try:
        check_tables_match(spark, args.years, FIRE_TABLES)  # stop after a killed Bronze run
        fires, quarantine = run_fire_silver(spark, args.years)
        save_fire_silver(spark, fires, quarantine, args.years)
        print(f"FIRE SILVER: saved {TABLE} and {QUARANTINE}")
        print_summary(spark, args.years)
        write_report(spark, args.years)
        add_to_summary("**Silver fire tables** (click a table to see its first rows)")
        show_tables(spark, [TABLE, QUARANTINE], args.years)
        return 0
    except Exception as error:
        print("FIRE SILVER FAILED:", error)
        report_failure("Silver fire", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
