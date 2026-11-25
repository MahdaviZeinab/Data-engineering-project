"""
Gold table for the risk map: one row per state and month.

  gold.state_month_risk   fire_count, avg_humidity, avg_temp, total_rain

Fires come from silver.fires (clean rows only), weather from silver.inmet_hourly.
The join starts from the fires, so a state with fires but no station is kept
(its weather columns are null).

Run from the inmet_weather folder:
  python -m gold.state_month_risk                  # all years, dev
  python -m gold.state_month_risk --years 2025
  python -m gold.state_month_risk --env prod
"""

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

from common.checks import has_columns, not_empty, one_row_per, run_checks
from common.report import report_failure
from common.spark import save_table, start_spark

TABLE = "gold.state_month_risk"
KEY = ["state", "source_year", "month"]


def read_silver(spark, years=None):
    # keep rows whose local day is in the file's year (1 Jan UTC can still be 31 Dec locally)
    weather = spark.table("silver.inmet_hourly")
    fires = spark.table("silver.fires")
    if years:
        weather = weather.where(F.col("source_year").isin(years))
        fires = fires.where(F.col("source_year").isin(years))
    same_year = F.year("local_date") == F.col("source_year")
    return weather.where(same_year), fires.where(same_year)


def monthly_fires(fires):
    clean = fires.where(F.col("dq_issue").isNull() & F.col("state").isNotNull())
    return (clean.withColumn("month", F.month("local_date"))
                 .groupBy(*KEY).agg(F.count("*").alias("fire_count")))


def monthly_weather(weather):
    # Per station first, then the average over the state's stations.
    # Rain is summed per station, so total_rain is a typical station's monthly total.
    per_station = (weather.withColumn("month", F.month("local_date"))
                          .groupBy("station_code", *KEY).agg(
                              F.avg("humidity_pct").alias("humidity"),
                              F.avg("temp_c").alias("temp"),
                              F.sum("precip_mm").alias("rain")))
    return per_station.groupBy(*KEY).agg(
        F.round(F.avg("humidity"), 1).alias("avg_humidity"),
        F.round(F.avg("temp"), 1).alias("avg_temp"),
        F.round(F.avg("rain"), 1).alias("total_rain"))


def build_state_month_risk(spark, years=None):
    weather, fires = read_silver(spark, years)
    table = monthly_fires(fires).join(monthly_weather(weather), KEY, "left")
    return table.select(*KEY, "fire_count", "avg_humidity", "avg_temp", "total_rain")


def save_state_month_risk(spark, table, years=None):
    run_checks(table, [not_empty, has_columns(*KEY, "fire_count"), one_row_per(*KEY)])
    save_table(spark, table, TABLE, years)


def main():
    parser = argparse.ArgumentParser(description="Build gold.state_month_risk from Silver.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("gold-state-month", lake, memory="2g")
    try:
        table = build_state_month_risk(spark, args.years)
        save_state_month_risk(spark, table, args.years)
        print(f"GOLD: saved {TABLE}")
        spark.table(TABLE).orderBy("source_year", "month", F.desc("fire_count")).show(20)
        return 0
    except Exception as error:
        print("GOLD STATE MONTH FAILED:", error)
        report_failure("Gold state month", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
