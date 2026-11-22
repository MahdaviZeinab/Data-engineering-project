"""
Bronze layer: load the INMET weather CSV files into Spark tables.

Bronze keeps the data as it is. We only:
  1. cut every line into its 19 values (all kept as text, nothing is changed),
  2. add where each row came from (file, line number) and when we loaded it,
  3. put bad lines in a quarantine table, with the reason: lines without 19 values,
     and all lines of a file whose header (line 9) is not what INMET promised.

Tables (all in the Spark database "bronze", split by year):
  bronze.inmet_hourly      one row per station and hour
  bronze.inmet_files       one row per CSV file: station info, row counts and a status
  bronze.inmet_quarantine  bad lines and the reason
  bronze.runs              one row per run: when, which years, how many files and rows

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

from common.spark import move_all_from_staging, save_to_staging, start_spark


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


# Step 1: read the files
# ---------------------------------------------------------------------------
def find_years(source, folder="inmet"):
    # Look in the raw folder and return the years that have this source's folder,
    # e.g. [2020, 2021] when raw/2020/inmet and raw/2021/inmet exist.
    # We use this when the user does not say which years to load.
    years = []
    for name in os.listdir(source):
        if name.isdigit() and len(name) == 4 and os.path.isdir(os.path.join(source, name, folder)):
            years.append(int(name))
    return sorted(years)


def read_files(spark, source, years, encoding=ENCODING, folder="inmet"):
    # Read every CSV file of the chosen years. We get one row per file,
    # with the file name, the year and the whole text of the file.
    # We read the bytes ourselves so we can decode them as Latin-1;
    # then words like "PRECIPITAÇÃO" stay correct.
    # The files are in <source>/<year>/inmet/. The fire job uses it too,
    # with encoding="UTF-8" and folder="fire".
    folders = [f"{source}/{year}/{folder}" for year in years]
    files = (spark.read.format("binaryFile")
             .option("pathGlobFilter", "*.[cC][sS][vV]")
             .load(folders))

    # The path looks like ".../raw/2020/inmet/INMET_..._A_31-12-2020.CSV":
    # the file name is the last part, the year is two parts before it.
    # Spark writes a space as "%20" in the path, url_decode turns it back.
    path_parts = F.split(F.expr("url_decode(path)"), "/")
    return files.select(
        F.element_at(path_parts, -1).alias("source_file"),
        F.element_at(path_parts, -3).cast("int").alias("source_year"),
        F.decode("content", encoding).alias("text"),
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


def check_header(files):
    # Check line 9 of every file: it must be the 19 column names INMET promised.
    # If not, the values could land in the wrong columns (rain in temperature),
    # so later all lines of that file go to quarantine.
    line_9 = F.element_at(F.split("text", "\r?\n"), 9)
    header = F.regexp_replace(line_9, ";$", "")      # remove the ";" at the end
    return files.withColumn("header_ok", F.coalesce(header == EXPECTED_HEADER, F.lit(False)))


def split_into_lines(files):
    # Turn "one row per file" into "one row per line of the file".
    # We also keep the line number, so every row can be found again in the file.
    lines = F.split("text", "\r?\n")
    return (files
            .select("source_year", "source_file", "station_code", "header_ok",
                    F.posexplode(lines).alias("index", "line"))
            .withColumn("source_line_number", F.col("index") + 1)
            .drop("index"))


def only_data_lines(lines):
    # Keep only the data lines: skip the 8 station lines, the header line
    # and empty lines (the file ends with an empty line).
    return lines.where((F.col("source_line_number") >= FIRST_DATA_LINE) & (F.col("line") != ""))


# Step 2: make the three tables
# ---------------------------------------------------------------------------
def has_19_values(line):
    # True for a good data line. INMET ends every line with ";",
    # so a good line gives 19 values, or 20 when we count the empty one at the end.
    # 20 values are only OK when the line ends with ";" (the 20th is empty).
    # If the 20th has a real value, the line goes to quarantine,
    # so we never drop a value without seeing it.
    number_of_values = F.size(F.split(line, ";"))
    ends_with_semicolon = F.col(line).endswith(";")
    return (number_of_values == 19) | ((number_of_values == 20) & ends_with_semicolon)


def find_problems(lines):
    # Give every data line a "problem" column. Empty (null) means the line is good.
    #   - the file has a wrong header      -> "unexpected header"
    #   - the line does not have 19 values -> "wrong number of values"
    problem = (F.when(~F.col("header_ok"), "unexpected header")
                .when(~has_19_values("line"), "wrong number of values"))
    return lines.withColumn("problem", problem)


def make_hourly(lines):
    # Make the hourly table from the good lines: one row per station and hour.
    # Each line is cut at ";" into the 19 values. The values stay text,
    # exactly as INMET wrote them (for example "25,5" or "-9999").
    good_lines = lines.where(F.col("problem").isNull())
    values = F.split("line", ";")
    columns = []
    for number, name in enumerate(COLUMNS, start=1):
        columns.append(F.element_at(values, number).alias(name))
    return good_lines.select("source_year", "station_code", "source_file",
                             "source_line_number", *columns)


def make_quarantine(lines):
    # Make the quarantine table: the lines that have a problem.
    # We keep the whole line and the reason, so we never lose data silently.
    bad_lines = lines.where(F.col("problem").isNotNull())
    return bad_lines.select("source_year", "station_code", "source_file",
                            "source_line_number",
                            F.col("line").alias("raw_line"), "problem")


def make_files_table(files, hourly, quarantine):
    # Make the files table: one row per CSV file, with the station info
    # from lines 1-8 and a simple check that the file is complete.
    lines = F.split("text", "\r?\n")
    table = files.select("source_year", "source_file", "station_code",
                         "start_date", "end_date", "header_ok", "text")

    # Lines 1-8 look like "ESTACAO:;BRASILIA". We keep the part after ";".
    for line_number, name in STATION_INFO.items():
        line = F.element_at(lines, line_number)
        table = table.withColumn(name, F.trim(F.split(line, ";").getItem(1)))
    table = table.drop("text")

    # How many hours should be in the file (24 per day), and how many we got.
    days = F.datediff("end_date", "start_date") + 1
    table = table.withColumn("expected_hours", days * 24)
    table = table.join(count_hours(hourly), "source_file", "left")
    table = table.join(count_bad_lines(quarantine), "source_file", "left")
    table = table.fillna(0, ["received_rows", "received_hours", "quarantined_lines"])
    table = table.withColumn("missing_hours", F.col("expected_hours") - F.col("received_hours"))
    return add_status(table)


def add_status(table):
    # Give every file one simple status, so we see at once if it is OK:
    #   QUARANTINED           wrong header, all lines went to quarantine
    #   INCOMPLETE            some hours are missing
    #   COMPLETE_WITH_ISSUES  all hours are there, but some are double or some lines were bad
    #   COMPLETE              everything is fine
    has_issues = ((F.col("received_rows") != F.col("received_hours"))
                  | (F.col("quarantined_lines") > 0))
    status = (F.when(~F.col("header_ok"), "QUARANTINED")
               .when(F.col("missing_hours") != 0, "INCOMPLETE")
               .when(has_issues, "COMPLETE_WITH_ISSUES")
               .otherwise("COMPLETE"))
    return table.withColumn("status", status)


def count_hours(hourly):
    # For each file: how many rows we got, and how many DIFFERENT hours.
    # If the same hour is in the file twice, rows is bigger than hours.
    return hourly.groupBy("source_file").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct("data", "hora_utc").alias("received_hours"))


def count_bad_lines(quarantine):
    # For each file: how many lines went to the quarantine table.
    return quarantine.groupBy("source_file").agg(F.count("*").alias("quarantined_lines"))


def add_load_info(table, run_id, load_time, source="inmet_portal"):
    # Add 4 columns that tell where and when this row was loaded.
    # This helps us later to find out which run made a row.
    # The fire job uses it too, with source="inpe_queimadas".
    return (table
            .withColumn("_bronze_source", F.lit(source))
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
    files = check_header(add_station_code(read_files(spark, source, years))).cache()
    lines = find_problems(only_data_lines(split_into_lines(files)))

    hourly = make_hourly(lines)
    quarantine = make_quarantine(lines)
    files_table = make_files_table(files, hourly, quarantine)
    tables = {
        "bronze.inmet_hourly": add_load_info(hourly, run_id, load_time),
        "bronze.inmet_quarantine": add_load_info(quarantine, run_id, load_time),
        "bronze.inmet_files": add_load_info(files_table, run_id, load_time),
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
    check_tables_match(spark, years)

    save_run_report(spark, run_id, load_time, years)
    print_summary(spark, years)


def check_new_tables(spark, years):
    # Check the new tables before they replace the old ones:
    #   - every year we load must have at least one file
    #     (otherwise we would delete that year and put nothing back)
    #   - every data line must be in the hourly table OR in quarantine
    files = spark.table("bronze.inmet_files_new")
    for year in years:
        if files.where(F.col("source_year") == year).count() == 0:
            raise ValueError(f"no files found for {year}; nothing was replaced")

    lines_in_files = files.agg(F.sum("received_rows") + F.sum("quarantined_lines")).first()[0]
    lines_saved = (spark.table("bronze.inmet_hourly_new").count()
                   + spark.table("bronze.inmet_quarantine_new").count())
    if lines_in_files != lines_saved:
        raise ValueError(f"expected {lines_in_files} lines but saved {lines_saved}; "
                         "nothing was replaced")


INMET_TABLES = ["inmet_hourly", "inmet_quarantine", "inmet_files"]


def check_tables_match(spark, years=None, tables=INMET_TABLES):
    # Check that the tables hold the SAME run for every year
    # (years=None: check all years).
    # This can only go wrong if the job was killed in the middle of step B
    # (e.g. the VM restarted). Silver calls this before it reads Bronze.
    # Fix: run Bronze again for the years in the message.
    # The fire job uses it too, with its own two tables.
    run_ids = None
    for name in tables:
        ids = spark.table(f"{DATABASE}.{name}").select("source_year", "_bronze_run_id")
        if years:
            ids = ids.where(F.col("source_year").isin(years))
        run_ids = ids if run_ids is None else run_ids.union(ids)
    mixed = (run_ids.groupBy("source_year")
             .agg(F.countDistinct("_bronze_run_id").alias("runs"))
             .where(F.col("runs") > 1).collect())
    if mixed:
        bad_years = sorted(r["source_year"] for r in mixed)
        raise ValueError(f"Bronze tables hold different runs for {bad_years}; "
                         "run Bronze again for these years")


def save_run_report(spark, run_id, load_time, years):
    # Add one row about this run to the table bronze.runs:
    # when it ran, which years, and how many files and rows it loaded.
    # So we can always see the history of all runs.
    files = spark.table("bronze.inmet_files").where(F.col("source_year").isin(years))
    report = files.agg(
        F.lit(run_id).alias("run_id"),
        F.lit(load_time).cast("timestamp").alias("run_time"),
        F.lit(" ".join(str(y) for y in years)).alias("years"),
        F.count("*").alias("files"),
        F.sum((F.col("status") == "COMPLETE").cast("int")).alias("complete_files"),
        F.sum((F.col("status") != "COMPLETE").cast("int")).alias("files_with_problems"),
        F.sum("received_rows").alias("hourly_rows"),
        F.sum("quarantined_lines").alias("quarantined_lines"),
    )
    report.write.mode("append").format("parquet").saveAsTable("bronze.runs")


def print_summary(spark, years):
    # Print a short report in the log: how many rows each table has now,
    # how many files have each status, and the files that need a look.
    for name in ["inmet_hourly", "inmet_files", "inmet_quarantine"]:
        rows = spark.table(f"{DATABASE}.{name}").where(F.col("source_year").isin(years)).count()
        print(f"BRONZE: {DATABASE}.{name} has {rows} rows for {years}")

    files = spark.table(f"{DATABASE}.inmet_files").where(F.col("source_year").isin(years))
    files.groupBy("status").count().show()
    problems = files.where(F.col("status") != "COMPLETE")
    print(f"BRONZE: {problems.count()} files need a look:")
    problems.select("source_year", "source_file", "status", "expected_hours",
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
