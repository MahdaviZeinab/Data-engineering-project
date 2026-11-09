"""
Bronze layer: load the INMET weather CSV files into Parquet with Spark.

Bronze rule: "Load first. Interpret later."
  1. Keep every value exactly as INMET wrote it (as text).
  2. Add where each row came from, and when.
  3. Check that the data arrived complete (expected vs received hours).
  4. Put bad lines aside with a reason. Never drop them silently.
  5. Running the job again gives the same result.

Output (in <output>/bronze/):
  inmet_hourly/      one row per station and hour, raw values
  inmet_files/       one row per source file: station info + delivery check
  inmet_quarantine/  lines we could not accept, with the reason
  _runs/<run>.json   short summary of each run

Run:
  python -m bronze.bronze_ingest                      # all years, dev
  python -m bronze.bronze_ingest --years 2025         # only 2025
  python -m bronze.bronze_ingest --env prod           # production folder
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# --------------------------------------------------------------------------
# What INMET delivers (the "contract")
# --------------------------------------------------------------------------
ENCODING = "ISO-8859-1"          # Latin-1: keeps "PRECIPITAÇÃO" correct
FIRST_DATA_LINE = 10             # lines 1-8 station info, line 9 header

# INMET_<region>_<state>_<station>_<name>_<start dd-mm-yyyy>_A_<end dd-mm-yyyy>.CSV
FILENAME = r"(?i)^INMET_([A-Z]+)_([A-Z]{2})_([A-Z0-9]+)_(.+)_(\d{2}-\d{2}-\d{4})_A_(\d{2}-\d{2}-\d{4})\.CSV$"

STATION_INFO = ["region", "state", "station_name", "wmo_code",
                "latitude", "longitude", "altitude", "foundation_date"]

# The INMET header (line 9). Bronze keeps these ORIGINAL Portuguese names
# as the column names (Spark 3.3+ allows spaces, commas and brackets in
# Parquet column names). Short English names are a Silver decision.
COLUMNS = [
    "Data",
    "Hora UTC",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)",
    "RADIACAO GLOBAL (Kj/m²)",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))",
    "VENTO, RAJADA MAXIMA (m/s)",
    "VENTO, VELOCIDADE HORARIA (m/s)",
]
EXPECTED_HEADER = ";".join(COLUMNS)


# --------------------------------------------------------------------------
# Step 1: read each file once, as raw bytes
# --------------------------------------------------------------------------
def read_files(spark, source, years):
    """One row per file: name, checksum, station info, header and all lines."""
    raw = (spark.read.format("binaryFile")
           .option("pathGlobFilter", "*.[cC][sS][vV]")
           .load([f"{source}/{y}" for y in years]))

    lines = F.split(F.decode("content", ENCODING), r"\r?\n")
    path = F.expr("url_decode(replace(path, '+', '%2B'))")   # "%20" -> " "
    df = raw.select(
        F.element_at(F.split(path, "/"), -1).alias("source_file"),
        # the year comes from the folder (data/raw/<year>/), not from the file
        # name: a bad or unusual file name must not move rows to another year
        F.element_at(F.split(path, "/"), -2).cast("int").alias("source_year"),
        F.sha2("content", 256).alias("file_sha256"),
        F.col("length").alias("file_bytes"),
        lines.alias("lines"),
    )

    def name_part(i):
        return F.regexp_extract("source_file", FILENAME, i)

    start = F.to_date(name_part(5), "dd-MM-yyyy")
    end = F.to_date(name_part(6), "dd-MM-yyyy")
    df = (df.withColumn("station_code", F.upper(name_part(3)))
            .withColumn("start_date", start)
            .withColumn("end_date", end)
            .withColumn("expected_rows", (F.datediff(end, start) + 1) * 24))

    # Lines 1-8 look like "ESTACAO:;BRASILIA" -> keep the value after ';'
    for i, name in enumerate(STATION_INFO, start=1):
        value = F.regexp_extract(F.element_at("lines", i), r"^[^;]*;([^;]*)", 1)
        df = df.withColumn(name, F.trim(value))

    header = F.regexp_replace(F.element_at("lines", 9), r";\s*$", "")
    return df.withColumn(
        "file_problem",
        F.when(~F.col("source_file").rlike(FILENAME), "bad_file_name")
         .when(F.size("lines") < FIRST_DATA_LINE, "no_data_lines")
         .when(header != EXPECTED_HEADER, "unexpected_header"),
    )


# --------------------------------------------------------------------------
# Step 2: split every hour line into its 19 values (kept as text)
# --------------------------------------------------------------------------
def split_lines(files):
    df = (files.select("source_year", "station_code", "source_file", "file_problem",
                       "start_date", "end_date",
                       F.posexplode("lines").alias("pos", "raw_line"))
               .withColumn("source_line_number", F.col("pos") + 1)
               .where((F.col("source_line_number") >= FIRST_DATA_LINE) & (F.col("raw_line") != "")))

    parts = F.split("raw_line", ";", -1)
    # 19 values, plus the empty one after the last ';'
    good_shape = (F.size(parts) == 19) | ((F.size(parts) == 20) & (F.element_at(parts, 20) == ""))
    df = df.withColumn("problem", F.coalesce(
        "file_problem", F.when(~good_shape, F.lit("wrong_number_of_values"))))

    for i, name in enumerate(COLUMNS, start=1):
        df = df.withColumn(name, F.element_at(parts, i))

    # Is this hour inside the dates written in the file name? (only for the check;
    # the "Data" value itself stays as text)
    day = F.to_date(F.col("Data"), "yyyy/MM/dd")
    return df.withColumn("in_date_range",
                         F.coalesce(day.between(F.col("start_date"), F.col("end_date")), F.lit(False)))


# --------------------------------------------------------------------------
# Step 3: delivery check per file (expected vs received)
# --------------------------------------------------------------------------
def check_files(files, lines):
    good = lines.where(F.col("problem").isNull())
    hour = F.concat_ws(" ", F.col("Data"), F.col("Hora UTC"))
    counts = good.groupBy("source_file").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct(hour).alias("distinct_hours"),
        # only hours inside the file name's dates count towards "complete"
        F.countDistinct(F.when(F.col("in_date_range"), hour)).alias("hours_in_range"),
        # rows outside those dates are reported on their own
        F.sum(F.when(F.col("in_date_range"), 0).otherwise(1)).alias("extra_hours"),
    )
    bad = lines.where(F.col("problem").isNotNull()).groupBy("source_file") \
               .agg(F.count("*").alias("quarantined_lines"))

    df = (files.drop("lines")
               .join(counts, "source_file", "left")
               .join(bad, "source_file", "left")
               .fillna(0, ["received_rows", "distinct_hours", "hours_in_range",
                           "extra_hours", "quarantined_lines"])
               .withColumn("duplicate_rows", F.col("received_rows") - F.col("distinct_hours"))
               .withColumn("missing_hours", F.col("expected_rows") - F.col("hours_in_range")))
    return df.withColumn(
        "status",
        F.when(F.col("file_problem").isNotNull(), "QUARANTINED")
         .when(F.col("missing_hours") > 0, "INCOMPLETE")
         .otherwise("COMPLETE"),
    ).drop("distinct_hours", "hours_in_range")


# --------------------------------------------------------------------------
# Step 4: write safely, replacing only the years of this run
# --------------------------------------------------------------------------
def replace_years(spark, df, path, years, expected_rows, run_id):
    """
    1. Write the new data to a temporary folder (the old data is not touched).
    2. Count the rows there. If the count is wrong, stop: the old data is still fine.
    3. Swap: move each old year folder away, move the new one in, delete the old one.
    Folders starting with "_" are ignored by Spark, so readers never see the temp data.
    """
    hadoop = spark._jvm.org.apache.hadoop.fs
    fs = hadoop.Path(path).getFileSystem(spark._jsc.hadoopConfiguration())
    temp, old = f"{path}/_new_{run_id}", f"{path}/_old_{run_id}"

    # 1. write to the temporary folder
    df.repartition("source_year").write.mode("overwrite").partitionBy("source_year").parquet(temp)

    # 2. check it before we touch the real table
    try:
        written = spark.read.parquet(temp).count()
    except AnalysisException:          # no rows at all (e.g. nothing quarantined)
        written = 0
    if written != expected_rows:
        fs.delete(hadoop.Path(temp), True)
        raise RuntimeError(f"{path}: wrote {written} rows, expected {expected_rows}. "
                           "Nothing was replaced.")

    # 3. swap the year folders
    fs.mkdirs(hadoop.Path(old))
    for y in years:
        current, fresh = hadoop.Path(f"{path}/source_year={y}"), hadoop.Path(f"{temp}/source_year={y}")
        if fs.exists(current):
            fs.rename(current, hadoop.Path(f"{old}/source_year={y}"))
        if fs.exists(fresh):
            fs.rename(fresh, current)
    fs.delete(hadoop.Path(old), True)
    fs.delete(hadoop.Path(temp), True)


def git_commit():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return "unknown"


def run_bronze(spark, source, output, years=None, run_id=None):
    if not years:
        years = sorted(int(d) for d in os.listdir(source) if d.isdigit() and len(d) == 4)
    now = datetime.now(timezone.utc)
    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ")
    bronze = f"{output}/bronze"

    # Read the CSV files only once; the next steps reuse this.
    files = read_files(spark, source, years).persist()
    lines = split_lines(files)
    manifest = check_files(files, lines)

    evidence = [
        F.lit("inmet_portal").alias("_bronze_source"),
        F.lit(run_id).alias("_bronze_run_id"),
        F.to_timestamp(F.lit(now.strftime("%Y-%m-%d %H:%M:%S"))).alias("_bronze_ingestion_time"),
        F.lit(git_commit()).alias("_bronze_code_version"),
    ]
    hourly = lines.where(F.col("problem").isNull()).select(
        "source_year", "station_code", "source_file", "source_line_number", *[F.col(f"`{c}`") for c in COLUMNS], *evidence)
    quarantine = lines.where(F.col("problem").isNotNull()).select(
        "source_year", "station_code", "source_file", "source_line_number",
        "raw_line", "problem", *evidence)

    # Delivery receipt for the whole run (also tells us how many rows to expect).
    row = manifest.agg(
        F.count("*").alias("files"),
        F.sum(F.when(F.col("status") == "COMPLETE", 1).otherwise(0)).alias("complete"),
        F.sum(F.when(F.col("status") == "INCOMPLETE", 1).otherwise(0)).alias("incomplete"),
        F.sum(F.when(F.col("status") == "QUARANTINED", 1).otherwise(0)).alias("quarantined"),
        F.sum("expected_rows").alias("expected_rows"),
        F.sum("received_rows").alias("received_rows"),
        F.sum("missing_hours").alias("missing_hours"),
        F.sum("extra_hours").alias("extra_hours"),
        F.sum("duplicate_rows").alias("duplicate_rows"),
        F.sum("quarantined_lines").alias("quarantined_lines"),
    ).first()
    report = {"run_id": run_id, "years": years, "time_utc": now.isoformat(timespec="seconds"),
              **{k: int(v or 0) for k, v in row.asDict().items()}}

    replace_years(spark, hourly, f"{bronze}/inmet_hourly", years,
                  report["received_rows"], run_id)
    replace_years(spark, quarantine, f"{bronze}/inmet_quarantine", years,
                  report["quarantined_lines"], run_id)
    replace_years(spark, manifest.select("*", *evidence), f"{bronze}/inmet_files", years,
                  report["files"], run_id)
    files.unpersist()

    os.makedirs(f"{bronze}/_runs", exist_ok=True)
    with open(f"{bronze}/_runs/{run_id}.json", "w") as f:
        json.dump(report, f, indent=2)
    print("BRONZE RUN:", json.dumps(report))
    return report


def main():
    parser = argparse.ArgumentParser(description="Load INMET CSV files into the Bronze layer.")
    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    parser.add_argument("--source", default=str(project / "data" / "raw"))
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    # Same rule as the course lab: CI can choose the folder with DAT535_OUTPUT_DIR.
    output = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = (SparkSession.builder.appName("inmet-bronze")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_bronze(spark, os.path.abspath(args.source), output, args.years)
        return 0
    except Exception as error:
        print("BRONZE FAILED:", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
