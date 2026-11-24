import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from bronze.bronze_ingest import add_load_info, check_tables_match, find_years, read_files
from common.report import add_to_summary, check_invalid_share, markdown_table, percent, report_failure
from common.spark import move_all_from_staging, save_to_staging, start_spark


# INPE file format: line 1 is the header, values split by ","
ENCODING = "UTF-8"
FIRST_DATA_LINE = 2
COLUMNS = ["id_bdq", "foco_id", "lat", "lon", "data_pas",
           "pais", "estado", "municipio", "bioma"]
EXPECTED_HEADER = ",".join(COLUMNS)

DATABASE = "bronze"
FIRE_TABLES = ["fire_foci", "fire_quarantine", "fire_files"]   # change together


# Step 1: read the files (read_files is from bronze_ingest)
def check_header(files):
    # bad header -> whole file goes to quarantine later
    line_1 = F.element_at(F.split("text", "\r?\n"), 1)
    header = F.regexp_replace(line_1, "^﻿", "")    # strip BOM if there is one
    return files.withColumn("header_ok", F.coalesce(header == EXPECTED_HEADER, F.lit(False)))


def split_into_lines(files):
    # one row per line, keep the line number
    lines = F.split("text", "\r?\n")
    return (files
            .select("source_year", "source_file", "header_ok",
                    F.posexplode(lines).alias("index", "line"))
            .withColumn("source_line_number", F.col("index") + 1)
            .drop("index"))


def only_data_lines(lines):
    # skip header and empty lines
    return lines.where((F.col("source_line_number") >= FIRST_DATA_LINE) & (F.col("line") != ""))


# Step 2: build the tables
def has_9_values(line):
    # no quotes in INPE files, so splitting on "," is safe
    return F.size(F.split(line, ",")) == len(COLUMNS)


def find_problems(lines):
    # problem is null for good lines
    problem = (F.when(~F.col("header_ok"), "unexpected header")
                .when(~has_9_values("line"), "wrong number of values"))
    return lines.withColumn("problem", problem)


def make_foci(lines):
    # values stay text as INPE wrote them, spaces included
    good_lines = lines.where(F.col("problem").isNull())
    values = F.split("line", ",")
    columns = []
    for number, name in enumerate(COLUMNS, start=1):
        columns.append(F.element_at(values, number).alias(name))
    return good_lines.select("source_year", "source_file", "source_line_number", *columns)


def make_quarantine(lines):
    bad_lines = lines.where(F.col("problem").isNotNull())
    return bad_lines.select("source_year", "source_file", "source_line_number",
                            F.col("line").alias("raw_line"), "problem")


def make_files_table(files, foci, quarantine):
    # one file per year. we can't know how many rows to expect,
    # so only header, bad lines and duplicate ids are checked
    table = files.select("source_year", "source_file", "header_ok")
    table = table.join(count_foci(foci), "source_file", "left")
    table = table.join(count_bad_lines(quarantine), "source_file", "left")
    table = table.fillna(0, ["received_rows", "received_foci", "quarantined_lines"])
    return add_status(table)


def add_status(table):
    # COMPLETE_WITH_ISSUES = duplicate fires or some bad lines
    has_issues = ((F.col("received_rows") != F.col("received_foci"))
                  | (F.col("quarantined_lines") > 0))
    status = (F.when(~F.col("header_ok"), "QUARANTINED")
               .when(has_issues, "COMPLETE_WITH_ISSUES")
               .otherwise("COMPLETE"))
    return table.withColumn("status", status)


def count_foci(foci):
    # rows > foci means some fires are duplicated
    return foci.groupBy("source_file").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct("foco_id").alias("received_foci"))


def count_bad_lines(quarantine):
    return quarantine.groupBy("source_file").agg(F.count("*").alias("quarantined_lines"))


# Main job
def run_fire_bronze(spark, source, years=None, run_id=None):
    if not years:
        years = find_years(source, folder="fire")
    now = datetime.now(timezone.utc)
    load_time = now.strftime("%Y-%m-%d %H:%M:%S")
    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ")
    print(f"FIRE BRONZE: loading years {years} (run {run_id})")

    # cache, files is used several times
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

    # write to _new tables first and check them
    for name, table in tables.items():
        save_to_staging(spark, table, name)
    files.unpersist()
    check_new_tables(spark, years)

    # then swap into the real tables (all three or none)
    move_all_from_staging(spark, list(tables), years)
    check_tables_match(spark, years, FIRE_TABLES)

    save_run_report(spark, run_id, load_time, years)
    print_summary(spark, years)


def check_new_tables(spark, years):
    # each year needs a file, and every line must be in foci or quarantine
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
    # append one row per run to bronze.fire_runs
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
    for name in ["fire_foci", "fire_files", "fire_quarantine"]:
        rows = spark.table(f"{DATABASE}.{name}").where(F.col("source_year").isin(years)).count()
        print(f"FIRE BRONZE: {DATABASE}.{name} has {rows} rows for {years}")

    files = spark.table(f"{DATABASE}.fire_files").where(F.col("source_year").isin(years))
    files.select("source_year", "source_file", "status", "received_rows",
                 "received_foci", "quarantined_lines") \
         .orderBy("source_year").show(truncate=False)


def write_report(spark, years):
    # The fire Bronze result per year on the GitHub run page (see common/report.py).
    files = spark.table(f"{DATABASE}.fire_files").where(F.col("source_year").isin(years))
    rows = []
    for r in files.orderBy("source_year").collect():
        bad_pct = percent(r["quarantined_lines"], r["received_rows"] + r["quarantined_lines"])
        status = check_invalid_share("Bronze fire", "bronze.fire_foci", r["source_year"], bad_pct)
        rows.append((r["source_year"], r["source_file"].split("/")[-1], r["status"],
                     r["received_rows"], r["received_foci"], r["quarantined_lines"], bad_pct, status))
    add_to_summary("### Bronze fire\n\n" + markdown_table(
        ["year", "file", "file status", "rows", "different fires", "bad lines", "bad %", "status"],
        rows))


def main():
    parser = argparse.ArgumentParser(description="Load INPE fire CSV files into Bronze tables.")
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    parser.add_argument("--source", default=str(project / "data" / "raw"))
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # CI can override the lake folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("fire-bronze", lake)
    try:
        source = os.path.abspath(args.source)
        years = args.years or find_years(source, folder="fire")
        run_fire_bronze(spark, source, years)
        write_report(spark, years)             # the result on the GitHub run page
        return 0
    except Exception as error:
        print("FIRE BRONZE FAILED:", error)
        report_failure("Bronze fire", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
