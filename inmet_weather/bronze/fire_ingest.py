"""
Bronze layer for our second source: INPE fire foci (focos de queimadas).

Same strategy as the INMET Bronze job (bronze_ingest.py). Bronze keeps the data as it is.
We only:
  1. cut every line into its 9 values (all kept as text, even the extra spaces),
  2. add where each row came from (file, line number) and when we loaded it,
  3. put bad lines in a quarantine table, with the reason: lines without 9 values,
     and all lines of a file whose header (line 1) is not what we expect.

Tables (all in the Spark database "bronze", split by year):
  bronze.fire_foci         one row per fire focus
  bronze.fire_files        one row per CSV file (= one year): row counts and a status
  bronze.fire_quarantine   bad lines and the reason
  bronze.fire_runs         one row per run: when, which years, how many files and rows

Download the files first:  python bronze/injection.py --source fire
They are in data/raw/<year>/fire/, next to the INMET files in data/raw/<year>/inmet/.

Run (from the inmet_weather folder):
  python -m bronze.fire_ingest                  # all years in data/raw, dev
  python -m bronze.fire_ingest --years 2025     # only 2025
  python -m bronze.fire_ingest --env prod       # production lake
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from bronze.bronze_ingest import add_load_info, check_tables_match, find_years, read_files
from common.spark import move_all_from_staging, save_to_staging, start_spark


# ---------------------------------------------------------------------------
# What an INPE fire file looks like
# ---------------------------------------------------------------------------
# Line 1 is the header, every other line is one fire focus, values split by ",".
# Example: " 1422833677 ,af379e6c-...,   -3.650000 ,  -54.732000 ,2020-06-16 17:00:00,
#           Brasil,PARÁ,PLACAS,Amazônia"
ENCODING = "UTF-8"          # INPE files are UTF-8 (INMET files are Latin-1)
FIRST_DATA_LINE = 2         # line 1: column names
COLUMNS = ["id_bdq", "foco_id", "lat", "lon", "data_pas",
           "pais", "estado", "municipio", "bioma"]
EXPECTED_HEADER = ",".join(COLUMNS)

DATABASE = "bronze"
FIRE_TABLES = ["fire_foci", "fire_quarantine", "fire_files"]   # change together


# Step 1: read the files
# ---------------------------------------------------------------------------
# read_files() comes from bronze_ingest.py: one row per file with
# source_file, source_year and the whole text (here: UTF-8, folder "fire").

def check_header(files):
    # Check line 1 of every file: it must be the 9 column names we expect.
    # If not, the values could land in the wrong columns,
    # so later all lines of that file go to quarantine.
    line_1 = F.element_at(F.split("text", "\r?\n"), 1)
    header = F.regexp_replace(line_1, "^﻿", "")    # remove a UTF-8 BOM, if there is one
    return files.withColumn("header_ok", F.coalesce(header == EXPECTED_HEADER, F.lit(False)))


def split_into_lines(files):
    # Turn "one row per file" into "one row per line of the file".
    # We also keep the line number, so every row can be found again in the file.
    lines = F.split("text", "\r?\n")
    return (files
            .select("source_year", "source_file", "header_ok",
                    F.posexplode(lines).alias("index", "line"))
            .withColumn("source_line_number", F.col("index") + 1)
            .drop("index"))


def only_data_lines(lines):
    # Keep only the data lines: skip the header line and empty lines
    # (the file ends with an empty line).
    return lines.where((F.col("source_line_number") >= FIRST_DATA_LINE) & (F.col("line") != ""))


# Step 2: make the three tables
# ---------------------------------------------------------------------------
def has_9_values(line):
    # True for a good data line: exactly 9 values split by ",".
    # (INPE does not put quotes around values, so a "," always splits two values.)
    return F.size(F.split(line, ",")) == len(COLUMNS)


def find_problems(lines):
    # Give every data line a "problem" column. Empty (null) means the line is good.
    #   - the file has a wrong header     -> "unexpected header"
    #   - the line does not have 9 values -> "wrong number of values"
    problem = (F.when(~F.col("header_ok"), "unexpected header")
                .when(~has_9_values("line"), "wrong number of values"))
    return lines.withColumn("problem", problem)


def make_foci(lines):
    # Make the fire table from the good lines: one row per fire focus.
    # Each line is cut at "," into the 9 values. The values stay text,
    # exactly as INPE wrote them (for example "   -3.650000 ").
    good_lines = lines.where(F.col("problem").isNull())
    values = F.split("line", ",")
    columns = []
    for number, name in enumerate(COLUMNS, start=1):
        columns.append(F.element_at(values, number).alias(name))
    return good_lines.select("source_year", "source_file", "source_line_number", *columns)


def make_quarantine(lines):
    # Make the quarantine table: the lines that have a problem.
    # We keep the whole line and the reason, so we never lose data silently.
    bad_lines = lines.where(F.col("problem").isNotNull())
    return bad_lines.select("source_year", "source_file", "source_line_number",
                            F.col("line").alias("raw_line"), "problem")


def make_files_table(files, foci, quarantine):
    # Make the files table: one row per CSV file (one file = one year),
    # with how many rows we got and a status.
    # INPE does not tell us how many fires a file should have (INMET: 24 per day),
    # so we can not find missing rows; we check the header, bad lines and double ids.
    table = files.select("source_year", "source_file", "header_ok")
    table = table.join(count_foci(foci), "source_file", "left")
    table = table.join(count_bad_lines(quarantine), "source_file", "left")
    table = table.fillna(0, ["received_rows", "received_foci", "quarantined_lines"])
    return add_status(table)


def add_status(table):
    # Give every file one simple status, so we see at once if it is OK:
    #   QUARANTINED           wrong header, all lines went to quarantine
    #   COMPLETE_WITH_ISSUES  some fires are double, or some lines were bad
    #   COMPLETE              everything is fine
    has_issues = ((F.col("received_rows") != F.col("received_foci"))
                  | (F.col("quarantined_lines") > 0))
    status = (F.when(~F.col("header_ok"), "QUARANTINED")
               .when(has_issues, "COMPLETE_WITH_ISSUES")
               .otherwise("COMPLETE"))
    return table.withColumn("status", status)


def count_foci(foci):
    # For each file: how many rows we got, and how many DIFFERENT fire ids.
    # If the same fire is in the file twice, rows is bigger than foci.
    return foci.groupBy("source_file").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct("foco_id").alias("received_foci"))


def count_bad_lines(quarantine):
    # For each file: how many lines went to the quarantine table.
    return quarantine.groupBy("source_file").agg(F.count("*").alias("quarantined_lines"))


# The whole Bronze job
# ---------------------------------------------------------------------------
def run_fire_bronze(spark, source, years=None, run_id=None):
    # Do all the steps: read the files, make the three tables, save them.
    if not years:
        years = find_years(source, folder="fire")
    now = datetime.now(timezone.utc)
    load_time = now.strftime("%Y-%m-%d %H:%M:%S")
    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ")
    print(f"FIRE BRONZE: loading years {years} (run {run_id})")

    # read every file only once, the next steps all use it
    files = check_header(read_files(spark, source, years, ENCODING, folder="fire")).cache()
    lines = find_problems(only_data_lines(split_into_lines(files)))

    foci = make_foci(lines)
    quarantine = make_quarantine(lines)
    files_table = make_files_table(files, foci, quarantine)
    tables = {
        "bronze.fire_foci": add_load_info(foci, run_id, load_time, "inpe_queimadas"),
        "bronze.fire_quarantine": add_load_info(quarantine, run_id, load_time, "inpe_queimadas"),
        "bronze.fire_files": add_load_info(files_table, run_id, load_time, "inpe_queimadas"),
    }

    # Step A: write all three tables to "<name>_new" tables and check them.
    # If anything goes wrong here, the real tables are not touched.
    for name, table in tables.items():
        save_to_staging(spark, table, name)
    files.unpersist()
    check_new_tables(spark, years)

    # Step B: everything is fine, so now replace the years in the real tables.
    # All three change together: if one fails, the old rows go back in all three.
    move_all_from_staging(spark, list(tables), years)
    check_tables_match(spark, years, FIRE_TABLES)

    save_run_report(spark, run_id, load_time, years)
    print_summary(spark, years)


def check_new_tables(spark, years):
    # Check the new tables before they replace the old ones:
    #   - every year we load must have a file
    #     (otherwise we would delete that year and put nothing back)
    #   - every data line must be in the fire table OR in quarantine
    files = spark.table("bronze.fire_files_new")
    for year in years:
        if files.where(F.col("source_year") == year).count() == 0:
            raise ValueError(f"no fire file found for {year}; nothing was replaced")

    lines_in_files = files.agg(F.sum("received_rows") + F.sum("quarantined_lines")).first()[0]
    lines_saved = (spark.table("bronze.fire_foci_new").count()
                   + spark.table("bronze.fire_quarantine_new").count())
    if lines_in_files != lines_saved:
        raise ValueError(f"expected {lines_in_files} lines but saved {lines_saved}; "
                         "nothing was replaced")


def save_run_report(spark, run_id, load_time, years):
    # Add one row about this run to the table bronze.fire_runs:
    # when it ran, which years, and how many files and rows it loaded.
    files = spark.table("bronze.fire_files").where(F.col("source_year").isin(years))
    report = files.agg(
        F.lit(run_id).alias("run_id"),
        F.lit(load_time).cast("timestamp").alias("run_time"),
        F.lit(" ".join(str(y) for y in years)).alias("years"),
        F.count("*").alias("files"),
        F.sum((F.col("status") == "COMPLETE").cast("int")).alias("complete_files"),
        F.sum((F.col("status") != "COMPLETE").cast("int")).alias("files_with_problems"),
        F.sum("received_rows").alias("fire_rows"),
        F.sum("quarantined_lines").alias("quarantined_lines"),
    )
    report.write.mode("append").format("parquet").saveAsTable("bronze.fire_runs")


def print_summary(spark, years):
    # Print a short report in the log: how many rows each table has now,
    # and the status of every file.
    for name in ["fire_foci", "fire_files", "fire_quarantine"]:
        rows = spark.table(f"{DATABASE}.{name}").where(F.col("source_year").isin(years)).count()
        print(f"FIRE BRONZE: {DATABASE}.{name} has {rows} rows for {years}")

    files = spark.table(f"{DATABASE}.fire_files").where(F.col("source_year").isin(years))
    files.select("source_year", "source_file", "status", "received_rows",
                 "received_foci", "quarantined_lines") \
         .orderBy("source_year").show(truncate=False)


def main():
    # Read the command line options, start Spark and run the job.
    parser = argparse.ArgumentParser(description="Load INPE fire CSV files into Bronze tables.")
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    parser.add_argument("--source", default=str(project / "data" / "raw"))
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # The lake folder keeps the Spark tables. CI can choose it with DAT535_OUTPUT_DIR.
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("fire-bronze", lake)
    try:
        run_fire_bronze(spark, os.path.abspath(args.source), args.years)
        return 0
    except Exception as error:
        print("FIRE BRONZE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
