"""Tests for the safe publish helper (S0)."""
import json

import pytest
from pyspark.sql import SparkSession

from common.publish import has_columns, not_empty, publish


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def test_good_run_replaces_table(spark, tmp_path):
    table = f"{tmp_path}/silver/t"
    publish(spark, spark.createDataFrame([(1,)], "x int"), table, "run1")
    rows = publish(spark, spark.createDataFrame([(1,), (2,)], "x int"), table, "run2")
    assert rows == {"all": 2}
    assert spark.read.parquet(table).count() == 2
    assert json.load(open(f"{table}/_published.json"))["run_id"] == "run2"


def test_failed_check_keeps_old_table(spark, tmp_path):
    table = f"{tmp_path}/silver/t"
    publish(spark, spark.createDataFrame([(1,)], "x int"), table, "run1")
    with pytest.raises(ValueError):
        publish(spark, spark.createDataFrame([], "x int"), table, "run2", checks=[not_empty])
    assert spark.read.parquet(table).count() == 1
    assert json.load(open(f"{table}/_published.json"))["run_id"] == "run1"


def test_missing_column_stops(spark, tmp_path):
    with pytest.raises(ValueError):
        publish(spark, spark.createDataFrame([(1,)], "x int"), f"{tmp_path}/silver/t", "r",
                checks=[has_columns("y")])
