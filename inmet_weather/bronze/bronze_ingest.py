import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from common.spark import move_all_from_staging, save_to_staging, start_spark


# INMET file format
ENCODING = "ISO-8859-1"     # INMET files are Latin-1, not UTF-8
FIRST_DATA_LINE = 10        # lines 1-8: station info, line 9: column names

# station info in lines 1-8, e.g. line 3 is "ESTACAO:;BRASILIA"
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

# INMET header name -> our column name (Spark does not allow commas or brackets)
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
def find_years(source, folder="inmet"):
    # years that have a raw/<year>/<folder>, used when --years is not given
    years = []
    for name in os.listdir(source):
        if name.isdigit() and len(name) == 4 and os.path.isdir(os.path.join(source, name, folder)):
            years.append(int(name))
    return sorted(years)


def read_files(spark, source, years, encoding=ENCODING, folder="inmet"):
    # one row per file with the whole text, decoded ourselves so Latin-1 works
    folders = [f"{source}/{year}/{folder}" for year in years]
    files = (spark.read.format("binaryFile")
             .option("pathGlobFilter", "*.[cC][sS][vV]")
             .load(folders))

    # path is .../raw/<year>/<folder>/<file>, url_decode fixes %20 for spaces
    path_parts = F.split(F.expr("url_decode(path)"), "/")
    return files.select(
        F.element_at(path_parts, -1).alias("source_file"),
        F.element_at(path_parts, -3).cast("int").alias("source_year"),
        F.decode("content", encoding).alias("text"),
    )


def add_station_code(files):
    # e.g. INMET_CO_DF_A001_BRASILIA_01-01-2025_A_31-12-2025.CSV
    # part 4 = station code, -3 = start date, -1 = end date
    name_parts = F.split(F.regexp_replace("source_file", "(?i)\\.csv$", ""), "_")
    return (files
            .withColumn("station_code", F.element_at(name_parts, 4))
            .withColumn("start_date", F.to_date(F.element_at(name_parts, -3), "dd-MM-yyyy"))
            .withColumn("end_date", F.to_date(F.element_at(name_parts, -1), "dd-MM-yyyy")))


def check_header(files):
    # bad header -> whole file goes to quarantine later
    line_9 = F.element_at(F.split("text", "\r?\n"), 9)
    header = F.regexp_replace(line_9, ";$", "")      # drop trailing ";"
    return files.withColumn("header_ok", F.coalesce(header == EXPECTED_HEADER, F.lit(False)))


def split_into_lines(files):
    # one row per line, keep the line number
    lines = F.split("text", "\r?\n")
    return (files
            .select("source_year", "source_file", "station_code", "header_ok",
                    F.posexplode(lines).alias("index", "line"))
            .withColumn("source_line_number", F.col("index") + 1)
            .drop("index"))


def only_data_lines(lines):
    # skip station info, header and empty lines
    return lines.where((F.col("source_line_number") >= FIRST_DATA_LINE) & (F.col("line") != ""))


# Step 2: build the tables
def has_19_values(line):
    # lines end with ";" so 20 parts is ok only if the last one is empty
    number_of_values = F.size(F.split(line, ";"))
    ends_with_semicolon = F.col(line).endswith(";")
    return (number_of_values == 19) | ((number_of_values == 20) & ends_with_semicolon)


def find_problems(lines):
    # problem is null for good lines
    problem = (F.when(~F.col("header_ok"), "unexpected header")
                .when(~has_19_values("line"), "wrong number of values"))
    return lines.withColumn("problem", problem)


def make_hourly(lines):
    # values stay text as INMET wrote them, e.g. "25,5" or "-9999"
    good_lines = lines.where(F.col("problem").isNull())
    values = F.split("line", ";")
    columns = []
    for number, name in enumerate(COLUMNS, start=1):
        columns.append(F.element_at(values, number).alias(name))
    return good_lines.select("source_year", "station_code", "source_file",
                             "source_line_number", *columns)


def make_quarantine(lines):
    bad_lines = lines.where(F.col("problem").isNotNull())
    return bad_lines.select("source_year", "station_code", "source_file",
                            "source_line_number",
                            F.col("line").alias("raw_line"), "problem")


def make_files_table(files, hourly, quarantine):
    # one row per file with station info and row counts
    lines = F.split("text", "\r?\n")
    table = files.select("source_year", "source_file", "station_code",
                         "start_date", "end_date", "header_ok", "text")

    # keep the part after ";"
    for line_number, name in STATION_INFO.items():
        line = F.element_at(lines, line_number)
        table = table.withColumn(name, F.trim(F.split(line, ";").getItem(1)))
    table = table.drop("text")

    # 24 hours per day
    days = F.datediff("end_date", "start_date") + 1
    table = table.withColumn("expected_hours", days * 24)
    table = table.join(count_hours(hourly), "source_file", "left")
    table = table.join(count_bad_lines(quarantine), "source_file", "left")
    table = table.fillna(0, ["received_rows", "received_hours", "quarantined_lines"])
    table = table.withColumn("missing_hours", F.col("expected_hours") - F.col("received_hours"))
    return add_status(table)


def add_status(table):
    # COMPLETE_WITH_ISSUES = duplicate hours or some bad lines
    has_issues = ((F.col("received_rows") != F.col("received_hours"))
                  | (F.col("quarantined_lines") > 0))
    status = (F.when(~F.col("header_ok"), "QUARANTINED")
               .when(F.col("missing_hours") != 0, "INCOMPLETE")
               .when(has_issues, "COMPLETE_WITH_ISSUES")
               .otherwise("COMPLETE"))
    return table.withColumn("status", status)


def count_hours(hourly):
    # rows > hours means some hours are duplicated
    return hourly.groupBy("source_file").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct("data", "hora_utc").alias("received_hours"))


def count_bad_lines(quarantine):
    return quarantine.groupBy("source_file").agg(F.count("*").alias("quarantined_lines"))


def add_load_info(table, run_id, load_time, source="inmet_portal"):
    # lineage columns (also used by fire_ingest)
    return (table
            .withColumn("_bronze_source", F.lit(source))
            .withColumn("_bronze_run_id", F.lit(run_id))
            .withColumn("_bronze_ingestion_time", F.lit(load_time).cast("timestamp"))
            .withColumn("_bronze_code_version", F.lit(git_version())))


def git_version():
    # short commit hash
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return "unknown"


# Main job
def run_bronze(spark, source, years=None, run_id=None):
    if not years:
        years = find_years(source)
    now = datetime.now(timezone.utc)
    load_time = now.strftime("%Y-%m-%d %H:%M:%S")
    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ")
    print(f"BRONZE: loading years {years} (run {run_id})")

    # cache, files is used several times
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

    # write to _new tables first and check them
    for name, table in tables.items():
        save_to_staging(spark, table, name)
    files.unpersist()
    check_new_tables(spark, years)

    # then swap into the real tables (all three or none)
    move_all_from_staging(spark, list(tables), years)
    check_tables_match(spark, years)

    save_run_report(spark, run_id, load_time, years)
    print_summary(spark, years)


def check_new_tables(spark, years):
    # each year needs files, and every line must be in hourly or quarantine
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
    # all tables should have the same run id per year.
    # only fails if the job died during the swap, rerun those years then
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
    # append one row per run to bronze.runs
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
    parser = argparse.ArgumentParser(description="Load INMET CSV files into the Bronze tables.")
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    parser.add_argument("--source", default=str(project / "data" / "raw"))
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # CI can override the lake folder
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
