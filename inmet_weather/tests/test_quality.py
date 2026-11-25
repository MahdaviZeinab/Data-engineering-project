"""Tests for Silver 10 (reconcile, checksum, rules) and Silver 11 (quality report)."""
import pytest
from pyspark.sql import SparkSession

from silver.quality import check_silver, checksum, reconcile


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# reconcile
def test_reconcile_ok():
    counts = reconcile(rows_in={2021: 10}, rows_out={2021: 7},
                       rows_quarantined={2021: 2}, duplicates={2021: 1})
    assert counts[2021]["good_rows_saved_in_silver"] == 7


def test_reconcile_stops_when_a_row_is_lost():
    with pytest.raises(ValueError, match="Reconcile failed"):
        reconcile(rows_in={2021: 10}, rows_out={2021: 6},          # 6 + 2 + 1 = 9, not 10
                  rows_quarantined={2021: 2}, duplicates={2021: 1})


# checksum
def table(spark, temp, run_id):
    return spark.createDataFrame([("A001", 2021, temp, run_id)],
                                 "station_code string, source_year int, temp_c double, "
                                 "_silver_run_id string")


def test_checksum_ignores_run_id(spark):
    assert checksum(table(spark, 25.0, "run1")) == checksum(table(spark, 25.0, "run2"))


def test_checksum_sees_a_changed_value(spark):
    assert checksum(table(spark, 25.0, "run1")) != checksum(table(spark, 26.0, "run1"))


# rules for good rows
def good_rows(spark, rows):
    return spark.createDataFrame(rows, "station_code string, observed_at_utc string, "
                                       "state string, timezone string")


def test_check_silver_ok(spark):
    check_silver(good_rows(spark, [("A001", "h1", "SP", "America/Sao_Paulo"),
                                   ("A001", "h2", "SP", "America/Sao_Paulo")]))


def test_check_silver_stops_on_double_hour(spark):
    with pytest.raises(ValueError, match="duplicate"):
        check_silver(good_rows(spark, [("A001", "h1", "SP", "America/Sao_Paulo"),
                                       ("A001", "h1", "SP", "America/Sao_Paulo")]))


def test_check_silver_stops_on_empty_state(spark):
    with pytest.raises(ValueError, match="empty state"):
        check_silver(good_rows(spark, [("A001", "h1", None, "America/Sao_Paulo")]))
