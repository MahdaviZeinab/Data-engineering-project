"""
Bronze layer: load the INMET weather CSV files into Spark tables.

Bronze keeps the data as it is. We only:
  1. cut every line into its 19 values (all kept as text, nothing is changed),
  2. add where each row came from (file, line number) and when we loaded it,
  3. put lines we can not cut into 19 values in a quarantine table.

Tables (all in the Spark database "bronze", split by year):
  bronze.inmet_hourly      one row per station and hour
  bronze.inmet_files       one row per CSV file: station info and row counts
  bronze.inmet_quarantine  lines with the wrong number of values

Silver reads them by name, for example:  spark.table("bronze.inmet_hourly")

Run (from the inmet_weather folder):
  python -m bronze.bronze_ingest                  # all years, dev
  python -m bronze.bronze_ingest --years 2025     # only 2025
  python -m bronze.bronze_ingest --env prod       # production lake
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from common.spark import start_spark


# ---------------------------------------------------------------------------
# What an INMET file looks like
# ---------------------------------------------------------------------------
ENCODING = "ISO-8859-1"     # INMET files are Latin-1, not UTF-8
FIRST_DATA_LINE = 10        # lines 1-8: station info, line 9: column names

# Lines 1-8 of every file, for example line 3 is "ESTACAO:;BRASILIA"
STATION_INFO = {
    1: "region",
    2: "state",
    3: "station_name",
    4: "wmo_code",
    5: "latitude",
    6: "longitude",
    7: "altitude",
    8: "foundation_date",
}

# The 19 columns of line 9: the name INMET writes -> the name in our table.
# Spark tables do not allow commas or brackets in a column name, so we use a
# simple version of the Portuguese name. The VALUES are not changed at all.
# Silver gives the columns short English names.
INMET_COLUMNS = {
    "Data": "data",
    "Hora UTC": "hora_utc",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "precipitacao_total_mm",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressao_estacao_mb",
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "pressao_max_mb",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "pressao_min_mb",
    "RADIACAO GLOBAL (Kj/m²)": "radiacao_global_kj_m2",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temperatura_ar_c",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "temperatura_orvalho_c",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "temperatura_max_c",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "temperatura_min_c",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "temperatura_orvalho_max_c",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "temperatura_orvalho_min_c",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "umidade_max_pct",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "umidade_min_pct",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "umidade_relativa_pct",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "vento_direcao_graus",
    "VENTO, RAJADA MAXIMA (m/s)": "vento_rajada_ms",
    "VENTO, VELOCIDADE HORARIA (m/s)": "vento_velocidade_ms",
}
COLUMNS = list(INMET_COLUMNS.values())           # our 19 column names
EXPECTED_HEADER = ";".join(INMET_COLUMNS)        # line 9 as INMET writes it

DATABASE = "bronze"


# ---------------------------------------------------------------------------
# Step 1: read the files
# ---------------------------------------------------------------------------
def find_years(source):
    # Look in the raw folder and return the year folders, e.g. [2020, 2021].
    # We use this when the user does not say which years to load.
    years = []
    for name in os.listdir(source):
        if name.isdigit() and len(name) == 4:
            years.append(int(name))
    return sorted(years)


def read_files(spark, source, years):
    # Read every CSV file of the chosen years. We get one row per file,
    # with the file name, the year and the whole text of the file.
    # We read the bytes ourselves so we can decode them as Latin-1;
    # then words like "PRECIPITAÇÃO" stay correct.
    folders = [f"{source}/{year}" for year in years]
    files = (spark.read.format("binaryFile")
             .option("pathGlobFilter", "*.[cC][sS][vV]")
             .load(folders))

    # The path looks like ".../raw/2020/INMET_..._A_31-12-2020.CSV".
    # Spark writes a space as "%20" in the path, url_decode turns it back.
    path_parts = F.split(F.expr("url_decode(path)"), "/")
    return files.select(
        F.element_at(path_parts, -1).alias("source_file"),
        F.element_at(path_parts, -2).cast("int").alias("source_year"),
        F.decode("content", ENCODING).alias("text"),
    )


def add_station_code(files):
    # Get the station code and the dates from the file name.
    # Example: INMET_CO_DF_A001_BRASILIA_01-01-2025_A_31-12-2025.CSV
    #   part 4 is the station code (A001),
    #   the 3rd part from the end is the first day, the last part is the last day.
    name_parts = F.split(F.regexp_replace("source_file", "(?i)\\.csv$", ""), "_")
    return (files
            .withColumn("station_code", F.element_at(name_parts, 4))
            .withColumn("start_date", F.to_date(F.element_at(name_parts, -3), "dd-MM-yyyy"))
            .withColumn("end_date", F.to_date(F.element_at(name_parts, -1), "dd-MM-yyyy")))


def split_into_lines(files):
    # Turn "one row per file" into "one row per line of the file".
    # We also keep the line number, so every row can be found again in the file.
    lines = F.split("text", "\r?\n")
    return (files
            .select("source_year", "source_file", "station_code",
                    F.posexplode(lines).alias("index", "line"))
            .withColumn("source_line_number", F.col("index") + 1)
            .drop("index"))


def only_data_lines(lines):
    # Keep only the data lines: skip the 8 station lines, the header line
    # and empty lines (the file ends with an empty line).
    return lines.where((F.col("source_line_number") >= FIRST_DATA_LINE) & (F.col("line") != ""))


# ---------------------------------------------------------------------------
# Step 2: make the three tables
# ---------------------------------------------------------------------------
def has_19_values(line):
    # True for a good data line. INMET ends every line with ";",
    # so a good line gives 19 values, or 20 when we count the empty one at the end.
    number_of_values = F.size(F.split(line, ";"))
    return number_of_values.isin(19, 20)


def make_hourly(lines):
    # Make the hourly table: one row per station and hour.
    # Each line is cut at ";" into the 19 values. The values stay text,
    # exactly as INMET wrote them (for example "25,5" or "-9999").
    good_lines = lines.where(has_19_values("line"))
    values = F.split("line", ";")
    columns = []
    for number, name in enumerate(COLUMNS, start=1):
        columns.append(F.element_at(values, number).alias(name))
    return good_lines.select("source_year", "station_code", "source_file",
                             "source_line_number", *columns)


def make_quarantine(lines):
    # Make the quarantine table: the data lines we could NOT cut into 19 values.
    # We keep the whole line and the reason, so we never lose data silently.
    bad_lines = lines.where(~has_19_values("line"))
    return bad_lines.select("source_year", "station_code", "source_file",
                            "source_line_number",
                            F.col("line").alias("raw_line"),
                            F.lit("wrong number of values").alias("problem"))


def make_files_table(files, hourly, quarantine):
    # Make the files table: one row per CSV file, with the station info
    # from lines 1-8 and a simple check that the file is complete.
    lines = F.split("text", "\r?\n")
    table = files.select("source_year", "source_file", "station_code",
                         "start_date", "end_date", "text")

    # Lines 1-8 look like "ESTACAO:;BRASILIA". We keep the part after ";".
    for line_number, name in STATION_INFO.items():
        line = F.element_at(lines, line_number)
        table = table.withColumn(name, F.trim(F.split(line, ";").getItem(1)))

    # Line 9 should be the 19 column names INMET promised.
    header = F.regexp_replace(F.element_at(lines, 9), ";$", "")
    table = table.withColumn("header_ok", header == EXPECTED_HEADER).drop("text")

    # How many hours should be in the file (24 per day), and how many we got.
    days = F.datediff("end_date", "start_date") + 1
    table = table.withColumn("expected_hours", days * 24)
    table = table.join(count_hours(hourly), "source_file", "left")
    table = table.join(count_bad_lines(quarantine), "source_file", "left")
    table = table.fillna(0, ["received_rows", "received_hours", "quarantined_lines"])
    return table.withColumn("missing_hours", F.col("expected_hours") - F.col("received_hours"))


def count_hours(hourly):
    # For each file: how many rows we got, and how many DIFFERENT hours.
    # If the same hour is in the file twice, rows is bigger than hours.
    return hourly.groupBy("source_file").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct("data", "hora_utc").alias("received_hours"))


def count_bad_lines(quarantine):
    # For each file: how many lines went to the quarantine table.
    return quarantine.groupBy("source_file").agg(F.count("*").alias("quarantined_lines"))


def add_load_info(table, run_id, load_time):
    # Add 4 columns that tell where and when this row was loaded.
    # This helps us later to find out which run made a row.
    return (table
            .withColumn("_bronze_source", F.lit("inmet_portal"))
            .withColumn("_bronze_run_id", F.lit(run_id))
            .withColumn("_bronze_ingestion_time", F.lit(load_time).cast("timestamp"))
            .withColumn("_bronze_code_version", F.lit(git_version())))


def git_version():
    # The short Git commit of our code, so we know which code made the data.
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Step 3: save the tables in Spark
# ---------------------------------------------------------------------------
def save_table(spark, table, name, years):
    # Save a table in Spark, split into one part per year (source_year).
    # If the table is already there, we first delete the years we load again,
    # then add the new rows. The other years are not touched.
    # So running the job twice gives the same result (no double rows).
    full_name = f"{DATABASE}.{name}"
    if spark.catalog.tableExists(full_name):
        for year in years:
            spark.sql(f"ALTER TABLE {full_name} DROP IF EXISTS PARTITION (source_year={year})")
    (table.write
          .mode("append")
          .format("parquet")
          .partitionBy("source_year")
          .saveAsTable(full_name))


# ---------------------------------------------------------------------------
# The whole Bronze job
# ---------------------------------------------------------------------------
def run_bronze(spark, source, years=None, run_id=None):
    # Do all the steps: read the files, make the three tables, save them.
    if not years:
        years = find_years(source)
    now = datetime.now(timezone.utc)
    load_time = now.strftime("%Y-%m-%d %H:%M:%S")
    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ")
    print(f"BRONZE: loading years {years} (run {run_id})")

    # read every file only once, the next steps all use it
    files = add_station_code(read_files(spark, source, years)).cache()
    lines = only_data_lines(split_into_lines(files))

    hourly = make_hourly(lines)
    quarantine = make_quarantine(lines)
    files_table = make_files_table(files, hourly, quarantine)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    save_table(spark, add_load_info(hourly, run_id, load_time), "inmet_hourly", years)
    save_table(spark, add_load_info(quarantine, run_id, load_time), "inmet_quarantine", years)
    save_table(spark, add_load_info(files_table, run_id, load_time), "inmet_files", years)
    files.unpersist()

    print_summary(spark, years)


def print_summary(spark, years):
    # Print a short report in the log: how many rows each table has now,
    # and which files have missing or double hours, bad lines or a strange header.
    for name in ["inmet_hourly", "inmet_files", "inmet_quarantine"]:
        rows = spark.table(f"{DATABASE}.{name}").where(F.col("source_year").isin(years)).count()
        print(f"BRONZE: {DATABASE}.{name} has {rows} rows for {years}")

    files = spark.table(f"{DATABASE}.inmet_files").where(F.col("source_year").isin(years))
    problems = files.where((F.col("missing_hours") != 0) | ~F.col("header_ok")
                           | (F.col("received_rows") != F.col("received_hours"))
                           | (F.col("quarantined_lines") > 0))
    print(f"BRONZE: {problems.count()} files need a look:")
    problems.select("source_year", "source_file", "header_ok", "expected_hours",
                    "received_hours", "missing_hours", "received_rows", "quarantined_lines") \
            .show(20, truncate=False)


def main():
    # Read the command line options, start Spark and run the job.
    parser = argparse.ArgumentParser(description="Load INMET CSV files into the Bronze tables.")
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    parser.add_argument("--source", default=str(project / "data" / "raw"))
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # The lake folder keeps the Spark tables. CI can choose it with DAT535_OUTPUT_DIR.
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("inmet-bronze", lake)
    try:
        run_bronze(spark, os.path.abspath(args.source), args.years)
        return 0
    except Exception as error:
        print("BRONZE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
