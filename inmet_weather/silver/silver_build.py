"""
Silver layer: turn the raw Bronze data into clean weather data.

Steps:
  1. read the Bronze tables
  2. check that Bronze has the columns we expect
  3. turn the text values into numbers ("25,5" -> 25.5, "-9999" -> empty)
  4. make the time: UTC time and the station's local time
  5. remove double rows (same station and same hour)
  6. impossible values -> empty, rare values -> flag (see value_rules.py)
  7. put rows we can not use aside, with a reason (see quarantine.py)
  8. make the station tables (see stations.py)
  9. save all four Silver tables together (if one fails, none is changed)
 10. write a small run summary (counts) to <lake>/_runs/silver_<run_id>.json

All rules (names, missing codes, time zones ...) are in silver/contract.py.

Run (from the inmet_weather folder):
  python -m silver.silver_build                   # all years, dev
  python -m silver.silver_build --years 2025      # only 2025
  python -m silver.silver_build --env prod        # production lake
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.checks import has_columns, not_empty, run_checks
from bronze.bronze_ingest import check_tables_match
from common.spark import move_all_from_staging, save_to_staging, start_spark
from silver import contract
from silver.quarantine import count_reasons, quarantine_rows
from silver.stations import STATION_TABLES, stage_stations, station_timezones
from silver.value_rules import apply_value_rules, count_flags


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


def count_conflicts(df):
    # Count the station-hours that are in Bronze more than once WITH DIFFERENT values
    # (a real conflict: two files say different things about the same hour).
    # 0 is a good result. If it is not 0, we look at a few examples in the report.
    rows = df.where(F.col("observed_at_utc").isNotNull())
    values = F.to_json(F.struct(*contract.MEASUREMENTS))      # all 17 values as one text
    per_key = rows.groupBy(*contract.KEY).agg(F.countDistinct(values).alias("versions"))
    return per_key.where(F.col("versions") > 1).count()


def remove_duplicates(df):
    # Keep only one row per station and hour. We keep the best row:
    #   1. the hour is inside the file's own dates. Every INMET file covers one year,
    #      so "same year as the file" is our simple test for this. The "extra hours"
    #      (for example 1 January of the next year) lose against the next year's file.
    #   2. it has fewer empty values
    #   3. it was loaded last
    #   4. file name and line number, so two runs always keep the same row
    # Rows without a time are all kept (we can not compare them).
    right_year = (F.year("observed_at_utc") == F.col("source_year")).cast("int")
    empty_values = sum(F.col(c).isNull().cast("int") for c in contract.MEASUREMENTS)
    best_first = (Window.partitionBy(*contract.KEY)
                  .orderBy(right_year.desc(), empty_values.asc(),
                           F.col("_bronze_ingestion_time").desc(),
                           F.col("source_file").asc(), F.col("source_line_number").asc()))

    df = df.withColumn("rank", F.row_number().over(best_first))
    keep = (F.col("rank") == 1) | F.col("observed_at_utc").isNull()
    return df.where(keep).drop("rank")


# ---------------------------------------------------------------------------
# Step 6: bad and rare values -> see silver/value_rules.py (apply_value_rules)
# Step 7: rows we can not use -> see silver/quarantine.py (quarantine_rows)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Next steps (empty for now, filled in by the next issues)
# ---------------------------------------------------------------------------
def add_station(df, files):
    # TODO Silver 10: add state, region and station name from the station tables.
    return df


# ---------------------------------------------------------------------------
# The whole Silver job
# ---------------------------------------------------------------------------
def run_silver(spark, years=None, run_id=None, summary=None):
    # Do all the cleaning steps, one after the other.
    # Returns two tables: (good rows, quarantined rows).
    # If you give a dict as "summary", the counts of this run are put in it.
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary = {} if summary is None else summary
    hourly, files = read_bronze(spark, years)

    df = check_schema(hourly)
    summary["bronze_rows"] = df.count()
    summary["not_numeric_values"] = count_not_numeric(df)
    print("SILVER: values that are not numbers:", summary["not_numeric_values"])
    df = cast_values(df)
    df = make_time(df, files)
    summary["duplicates_removed"] = count_duplicates(df)
    summary["duplicate_conflicts"] = count_conflicts(df)
    print("SILVER: double rows removed:", summary["duplicates_removed"],
          "| with different values:", summary["duplicate_conflicts"])
    df = remove_duplicates(df)
    df = apply_value_rules(df)
    print("SILVER: flagged values per year:")
    count_flags(df).show(200, truncate=False)
    good, quarantined = quarantine_rows(df, files)
    print("SILVER: quarantined rows per year and reason:")
    count_reasons(quarantined).show(200, truncate=False)
    good = add_station(good, files)

    run = F.lit(run_id)
    return good.withColumn("_silver_run_id", run), quarantined.withColumn("_silver_run_id", run)


def save_silver(spark, silver, years, run_id, quarantined=None):
    # Save all Silver tables TOGETHER, like Bronze does:
    #   step A: check the hourly table and write all tables to "<name>_new"
    #   step B: replace them all at once; if one fails, all get their old rows back.
    # The hourly table replaces only the years we ran,
    # the station tables are small and always made from all years.
    run_checks(silver, [not_empty, has_columns(*contract.KEY, *contract.MEASUREMENTS)])
    save_to_staging(spark, silver, "silver.inmet_hourly")
    stage_stations(spark, run_id)

    tables = {"silver.inmet_hourly": years}
    if quarantined is not None:                 # Silver 9: the rows we put aside
        save_to_staging(spark, quarantined, "silver.inmet_quarantine")
        tables["silver.inmet_quarantine"] = years
    for name in STATION_TABLES:
        tables[name] = None
    move_all_from_staging(spark, tables)


def quarantined_per_reason(spark):
    # {reason: number of rows} from silver.inmet_quarantine (empty if there is no table yet)
    if not spark.catalog.tableExists("silver.inmet_quarantine"):
        return {}
    rows = spark.table("silver.inmet_quarantine").groupBy("dq_issue").count().collect()
    return {r["dq_issue"]: r["count"] for r in rows}


def write_run_summary(lake, run_id, years, summary, spark):
    # Silver 3: save the numbers of this run as a small JSON file, one file per run,
    # so the report can show them and we can compare runs.
    # Example: <lake>/_runs/silver_20251119T180000Z.json
    result = spark.table("silver.inmet_hourly")
    summary = {
        "run_id": run_id,
        "years": years or "all",
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **summary,
        "silver_rows_per_year": {str(r["source_year"]): r["count"]
                                 for r in result.groupBy("source_year").count().collect()},
        "quarantined_per_reason": quarantined_per_reason(spark),
    }
    folder = Path(lake) / "_runs"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"silver_{run_id}.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print("SILVER: run summary saved in", path)
    return path


def print_summary(spark):
    # Print a short report in the log: rows per year and a few example rows.
    result = spark.table("silver.inmet_hourly")
    result.groupBy("source_year").count().orderBy("source_year").show()
    result.select("station_code", "observed_at_utc", "local_time", "timezone",
                  "temp_c", "humidity_pct", "precip_mm", "dq_flags", "source_year") \
          .show(10, truncate=False)


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
        # stop if Bronze was only half replaced (a killed Bronze run)
        check_tables_match(spark)

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        summary = {}
        silver, quarantined = run_silver(spark, args.years, run_id, summary)
        save_silver(spark, silver, args.years, run_id, quarantined)
        print("SILVER: saved silver.inmet_hourly, silver.inmet_quarantine, "
              "silver.station_history, silver.station")
        write_run_summary(lake, run_id, args.years, summary, spark)
        print_summary(spark)
        return 0
    except Exception as error:
        print("SILVER FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
