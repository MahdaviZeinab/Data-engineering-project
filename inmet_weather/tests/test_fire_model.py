"""
Tests for the fire model (logistic regression), with a small fake Gold table.
In the fake data, low humidity gives many fires, so the model must learn that.
Run:  python -m pytest -q tests/test_fire_model.py
"""
from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from ml.train_fire_model import (DAILY, METRICS, PREDICTIONS, THRESHOLDS, WEIGHTS,
                                 run_training, save_results)

DAILY_SCHEMA = ("state string, local_date date, source_year int, weather_ok boolean, "
                "humidity_min_pct double, temp_max_c double, rain_mm double, wind_max_ms double, "
                "days_without_rain int, risk_score int, fires long")


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


@pytest.fixture(scope="module")
def gold(spark):
    # One year, two states. Humidity goes up and down (a fixed pattern, no randomness);
    # dry days (humidity < 25 %) have many fires, the other days few.
    # The fire counts vary, like real data: if all dry days had the same count,
    # "more than the 90 % level" would never be true.
    rows = []
    for state, extra in [("MT", 0), ("PA", 5)]:
        for i in range(365):
            day = date(2025, 1, 1) + timedelta(days=i)
            humidity = 10.0 + (i * 37 + extra) % 70
            fires = (100 + 7 * i % 200) if humidity < 25 else (i % 5)   # dry days: 100-299 fires
            rows.append((state, day, 2025, True, humidity, 35.0 - humidity / 5,
                         0.0 if humidity < 40 else 5.0, 4.0, i % 20, 3 if humidity < 25 else 1, fires))
    spark.sql("DROP DATABASE IF EXISTS gold CASCADE")
    spark.sql("CREATE DATABASE gold")
    spark.createDataFrame(rows, DAILY_SCHEMA).write.saveAsTable(DAILY)


def test_model_learns_that_dry_days_have_more_fires(spark, gold):
    fitted, metrics, predictions, weights = run_training(spark, "2025-07-01", "2025-09-01", "t1")

    sets = {r["data_set"]: r for r in metrics.collect()}
    assert set(sets) == {"train", "validation", "test"}
    assert sets["test"]["auc"] > 0.9                       # an easy pattern: the model must find it
    assert sets["test"]["threshold"] in THRESHOLDS        # chosen on validation
    humidity = weights.where("feature = 'humidity_min_pct'").first()
    assert humidity["weight"] < 0 and humidity["odds_ratio"] < 1   # more humidity -> fewer fires


def test_test_set_is_only_the_test_period(spark, gold):
    _, _, predictions, _ = run_training(spark, "2025-07-01", "2025-09-01", "t2")
    first = predictions.agg({"local_date": "min"}).first()[0]
    assert first >= date(2025, 9, 1)


def test_results_are_saved(spark, gold, tmp_path):
    fitted, metrics, predictions, weights = run_training(spark, "2025-07-01", "2025-09-01", "t3")
    path = save_results(spark, str(tmp_path), fitted, metrics, predictions, weights)
    assert spark.table(METRICS).where("run_id = 't3'").count() == 3
    assert spark.table(PREDICTIONS).count() == predictions.count()
    assert spark.table(WEIGHTS).count() > 0
    assert (tmp_path / "models" / "fire_lr_t3").exists()


def test_wrong_dates_stop_the_job(spark, gold):
    with pytest.raises(ValueError, match="has no rows"):
        run_training(spark, "2030-01-01", "2031-01-01", "t4")      # nothing after 2025
