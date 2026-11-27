"""
Silver job for the INMET weather data: bronze.inmet_hourly -> silver.inmet_hourly.

Steps:
  1. read Bronze and check its columns
  2. text -> numbers ("25,5" -> 25.5, "-9999" -> null)
  3. UTC time and local time
  4. remove duplicate station-hours
  5. value rules (inmet_value_rules.py)
  6. quarantine unusable rows (inmet_quarantine.py)
  7. add station details
  8. reconcile row counts and build the quality report (quality.py)
  9. build the station tables (inmet_stations.py)
 10. save all tables together
 11. write a run summary to <lake>/_runs/silver_<run_id>.json

Run from the inmet_weather folder:
  python -m silver.inmet_build                   # all years, dev
  python -m silver.inmet_build --years 2025
  python -m silver.inmet_build --env prod
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
from common.report import (add_to_summary, check_invalid_share, markdown_table, percent,
                           report_failure, show_tables)
from common.spark import move_all_from_staging, save_to_staging, start_spark
from silver import inmet_contract as contract
from silver.quality import check_silver, checksum, quality_report, reconcile, rows_per_year
from silver.inmet_quarantine import count_reasons, quarantine_rows
from silver.inmet_stations import (STATION_TABLES, keep_one_file_per_year, stage_stations,
                             station_timezones)
from silver.inmet_value_rules import apply_value_rules, count_flags


# read Bronze
def read_bronze(spark, years=None):
    hourly = spark.table("bronze.inmet_hourly")
    files = spark.table("bronze.inmet_files")
    if years:
        hourly = hourly.where(F.col("source_year").isin(years))
        files = files.where(F.col("source_year").isin(years))
    return hourly, files


def check_schema(df):
    # fail fast if Bronze gained or lost a column
    contract.check_schema(df.columns)
    return df


# text -> numbers
def clean_text(column):
    # trim and use "." as decimal sign: "25,5" -> "25.5"
    return F.regexp_replace(F.trim(F.col(column)), ",", ".")


def cast_values(df):
    # cast each measurement to double under its English name; missing codes -> null
    for bronze_name, silver_name in contract.RENAME.items():
        text = clean_text(bronze_name)
        is_missing = text.isin(contract.MISSING_CODES)
        number = F.when(is_missing, None).otherwise(text.cast("double"))
        df = df.withColumn(silver_name, number).drop(bronze_name)
    return df


def count_not_numeric(df):
    # values that are neither a number nor a missing code (they end up as null)
    counts = []
    for bronze_name, silver_name in contract.RENAME.items():
        text = clean_text(bronze_name)
        is_missing = text.isin(contract.MISSING_CODES)
        not_a_number = text.isNotNull() & ~is_missing & text.cast("double").isNull()
        counts.append(F.sum(not_a_number.cast("int")).alias(silver_name))
    return df.agg(*counts).first().asDict()


# time
def utc_time():
    # "2025/01/01" + "0300 UTC" -> 2025-01-01 03:00; an invalid hour gives null
    day = F.regexp_replace("data", "-", "/")
    hhmm = F.regexp_replace("hora_utc", "[^0-9]", "")
    good_hour = hhmm.rlike("^([01][0-9]|2[0-3])[0-5][0-9]$")
    return F.when(good_hour, F.to_timestamp(F.concat_ws(" ", day, hhmm), "yyyy/MM/dd HHmm"))


def make_time(df, files):
    # local time is needed so daily totals use the station's own day
    df = df.withColumn("observed_at_utc", utc_time())
    df = df.join(F.broadcast(station_timezones(files)), "station_code", "left")
    df = df.withColumn("local_time", F.from_utc_timestamp("observed_at_utc", F.col("timezone")))
    df = df.withColumn("local_date", F.to_date("local_time"))
    df = df.withColumn("local_hour", F.hour("local_time"))
    problem = (F.when(F.col("observed_at_utc").isNull(), "bad_timestamp")
                .when(F.col("timezone").isNull(), "unknown_timezone"))
    return df.withColumn("dq_issue", problem)


# duplicates
def count_conflicts(df):
    # station-hours that appear more than once with different values
    rows = df.where(F.col("observed_at_utc").isNotNull())
    values = F.to_json(F.struct(*contract.MEASUREMENTS))
    per_key = rows.groupBy(*contract.KEY).agg(F.countDistinct(values).alias("versions"))
    return per_key.where(F.col("versions") > 1).count()


def remove_duplicates(df):
    # One row per station-hour. Preference order:
    #   1. hour falls in the file's own year (drops the overlap hours at year ends)
    #   2. fewer null values
    #   3. loaded most recently
    #   4. file name and line number, so reruns pick the same row
    # Rows without a timestamp are all kept.
    right_year = (F.year("observed_at_utc") == F.col("source_year")).cast("int")
    empty_values = sum(F.col(c).isNull().cast("int") for c in contract.MEASUREMENTS)
    best_first = (Window.partitionBy(*contract.KEY)
                  .orderBy(right_year.desc(), empty_values.asc(),
                           F.col("_bronze_ingestion_time").desc(),
                           F.col("source_file").asc(), F.col("source_line_number").asc()))

    df = df.withColumn("rank", F.row_number().over(best_first))
    keep = (F.col("rank") == 1) | F.col("observed_at_utc").isNull()
    return df.where(keep).drop("rank")


# station details
def add_station(df, files):
    # join on station + year so each hour gets the station info from that year's file
    stations = keep_one_file_per_year(files).select(
        "station_code", "source_year",
        F.upper(F.trim("state")).alias("state"),
        F.upper(F.trim("region")).alias("region"),
        F.trim("station_name").alias("station_name"))
    return df.join(F.broadcast(stations), ["station_code", "source_year"], "left")


# job
def run_silver(spark, years=None, run_id=None, summary=None):
    # Returns (good rows, quarantined rows, quality report). Counts go into `summary`.
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary = {} if summary is None else summary
    hourly, files = read_bronze(spark, years)
    # cached because it is read again for the counts and the quality report
    hourly = hourly.cache()

    df = check_schema(hourly)
    rows_in = rows_per_year(df)
    summary["bronze_rows"] = sum(rows_in.values())
    summary["not_numeric_values"] = count_not_numeric(df)
    print("SILVER: values that are not numbers:", summary["not_numeric_values"])
    df = cast_values(df)
    df = make_time(df, files)
    summary["duplicate_conflicts"] = count_conflicts(df)
    df = remove_duplicates(df).cache()         # window is the slowest step
    rows_left = rows_per_year(df)
    duplicates = {year: rows - rows_left.get(year, 0) for year, rows in rows_in.items()}
    summary["duplicates_removed"] = sum(duplicates.values())
    print("SILVER: double rows removed:", summary["duplicates_removed"],
          "| with different values:", summary["duplicate_conflicts"])
    df = apply_value_rules(df)
    print("SILVER: flagged values per year:")
    count_flags(df).show(200, truncate=False)
    good, quarantined = quarantine_rows(df, files)
    print("SILVER: quarantined rows per year and reason:")
    count_reasons(quarantined).show(200, truncate=False)
    good = add_station(good, files)

    run = F.lit(run_id)
    good = good.withColumn("_silver_run_id", run).cache()
    quarantined = quarantined.withColumn("_silver_run_id", run).cache()

    # stops here (before saving) if rows are lost or a rule is broken
    counts = reconcile(rows_in, rows_per_year(good), rows_per_year(quarantined), duplicates)
    df.unpersist()
    check_silver(good)
    summary["reconcile"] = counts
    summary["checksum"] = checksum(good)
    print("SILVER: reconcile ok:", counts, "| checksum:", summary["checksum"])

    report = quality_report(hourly, good, counts).withColumn("_silver_run_id", run)
    return good, quarantined, report


def save_silver(spark, silver, years, run_id, quarantined=None, report=None):
    # Write everything to "<name>_new" first, then swap all tables in one go
    # (rolled back if any swap fails). Hourly data replaces only the given years;
    # the station tables are rebuilt from all years.
    run_checks(silver, [not_empty, has_columns(*contract.KEY, *contract.MEASUREMENTS)])
    save_to_staging(spark, silver, "silver.inmet_hourly")
    stage_stations(spark, run_id)

    tables = {"silver.inmet_hourly": years}
    if quarantined is not None:
        save_to_staging(spark, quarantined, "silver.inmet_quarantine")
        tables["silver.inmet_quarantine"] = years
    if report is not None:
        save_to_staging(spark, report, "silver.quality_report")
        tables["silver.quality_report"] = years
    for name in STATION_TABLES:
        tables[name] = None
    move_all_from_staging(spark, tables)


def quarantined_per_reason(spark):
    if not spark.catalog.tableExists("silver.inmet_quarantine"):
        return {}
    rows = spark.table("silver.inmet_quarantine").groupBy("dq_issue").count().collect()
    return {r["dq_issue"]: r["count"] for r in rows}


def write_run_summary(lake, run_id, years, summary, spark):
    # one JSON file per run, e.g. <lake>/_runs/silver_20251119T180000Z.json
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
    result = spark.table("silver.inmet_hourly")
    result.groupBy("source_year").count().orderBy("source_year").show()
    result.select("station_code", "observed_at_utc", "local_time", "timezone",
                  "temp_c", "humidity_pct", "precip_mm", "dq_flags", "source_year") \
          .show(10, truncate=False)


def write_report(spark, counts, years=None):
    # Results table for the GitHub run page.
    # invalid % = quarantined / Bronze rows; duplicates are listed separately.
    rows = []
    for year, c in sorted(counts.items()):
        invalid_pct = percent(c["bad_rows_moved_to_quarantine"], c["rows_read_from_bronze"])
        status = check_invalid_share("Silver INMET", "silver.inmet_hourly", year, invalid_pct)
        rows.append((year, c["rows_read_from_bronze"], c["good_rows_saved_in_silver"],
                     c["bad_rows_moved_to_quarantine"], c["duplicate_rows_removed"],
                     invalid_pct, status))
    text = "### Silver INMET\n\n" + markdown_table(
        ["year", "rows from Bronze", "good rows", "quarantined", "duplicates removed",
         "invalid %", "status"], rows)

    quarantine = spark.table("silver.inmet_quarantine")
    if years:
        quarantine = quarantine.where(F.col("source_year").isin(years))
    reasons = (quarantine.groupBy("source_year", "dq_issue").count()
               .orderBy("source_year", F.desc("count")).collect())
    if reasons:
        text += "\n\nQuarantined rows by reason:\n\n" + markdown_table(
            ["year", "reason", "rows"], [tuple(r) for r in reasons])
    add_to_summary(text)


def main():
    parser = argparse.ArgumentParser(description="Build the Silver tables from Bronze.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # CI can override the lake folder with DAT535_OUTPUT_DIR
    project = Path(__file__).resolve().parents[1]
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-silver", lake, memory="2g")   # VM has 8 GB
    try:
        # stop if a Bronze run was killed halfway
        check_tables_match(spark)

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        summary = {}
        silver, quarantined, report = run_silver(spark, args.years, run_id, summary)
        save_silver(spark, silver, args.years, run_id, quarantined, report)
        print("SILVER: saved silver.inmet_hourly, silver.inmet_quarantine, "
              "silver.quality_report, silver.station_history, silver.station")
        spark.table("silver.quality_report").show(truncate=False)
        write_run_summary(lake, run_id, args.years, summary, spark)
        print_summary(spark)
        write_report(spark, summary["reconcile"], args.years)
        add_to_summary("**Silver INMET tables** (click a table to see its first rows)")
        show_tables(spark, ["silver.inmet_hourly", "silver.inmet_quarantine",
                            "silver.quality_report", *STATION_TABLES], args.years)
        return 0
    except Exception as error:
        print("SILVER FAILED:", error)
        report_failure("Silver INMET", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
