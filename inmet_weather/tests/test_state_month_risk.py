"""
Tests for gold.state_month_risk, with tiny fake Silver tables.
Run:  python -m pytest -q tests/test_state_month_risk.py
"""
from datetime import date

import pytest
from pyspark.sql import SparkSession

from gold.state_month_risk import TABLE, build_state_month_risk, save_state_month_risk

WEATHER_SCHEMA = ("station_code string, state string, local_date date, source_year int, "
                  "humidity_pct double, temp_c double, precip_mm double")
FIRE_SCHEMA = "foco_id string, state string, local_date date, source_year int, dq_issue string"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.warehouse.dir", str(warehouse))
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def empty_lake(spark):
    spark.sql("DROP DATABASE IF EXISTS silver CASCADE")
    spark.sql("DROP DATABASE IF EXISTS gold CASCADE")
    spark.sql("CREATE DATABASE silver")


def make_silver(spark, weather, fires):
    spark.createDataFrame(weather, WEATHER_SCHEMA).write.saveAsTable("silver.inmet_hourly")
    spark.createDataFrame(fires, FIRE_SCHEMA).write.saveAsTable("silver.fires")


def rows(spark):
    return {(r["state"], r["month"]): r for r in build_state_month_risk(spark).collect()}


def test_fires_and_weather_per_state_and_month(spark):
    sep1, sep2 = date(2025, 9, 1), date(2025, 9, 2)
    make_silver(spark,
                [("A001", "DF", sep1, 2025, 30.0, 28.0, 2.0),
                 ("A001", "DF", sep2, 2025, 20.0, 32.0, 3.0),
                 ("A002", "DF", sep1, 2025, 40.0, 30.0, 0.0)],
                [("f1", "DF", sep1, 2025, None), ("f2", "DF", sep2, 2025, None),
                 ("f3", "DF", sep2, 2025, "wrong_state")])
    row = rows(spark)[("DF", 9)]
    assert row["fire_count"] == 2                        # the quarantined fire is not counted
    assert row["avg_humidity"] == 32.5                   # stations: 25 and 40
    assert row["avg_temp"] == 30.0
    assert row["total_rain"] == 2.5                      # stations: 5 mm and 0 mm


def test_state_with_fires_but_no_station_is_kept(spark):
    d = date(2025, 8, 10)
    make_silver(spark, [("A001", "DF", d, 2025, 30.0, 28.0, 0.0)],
                [("f1", "AC", d, 2025, None)])
    row = rows(spark)[("AC", 8)]
    assert row["fire_count"] == 1
    assert row["avg_humidity"] is None and row["total_rain"] is None


def test_saved_table_has_one_row_per_state_and_month(spark):
    d = date(2025, 7, 5)
    make_silver(spark, [("A001", "GO", d, 2025, 50.0, 25.0, 1.0)],
                [("f1", "GO", d, 2025, None), ("f2", "GO", d, 2025, None)])
    save_state_month_risk(spark, build_state_month_risk(spark), [2025])
    saved = spark.table(TABLE).collect()
    assert [(r["state"], r["month"], r["fire_count"]) for r in saved] == [("GO", 7, 2)]
