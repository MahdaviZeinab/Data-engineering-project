"""Tests for Silver 9: rows we can not use go to quarantine, with one reason."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.inmet_contract import MEASUREMENTS
from silver.inmet_quarantine import count_reasons, quarantine_rows


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


T = "2021-01-01 05:00:00"
ZONE = "America/Sao_Paulo"


def hours(spark, rows):
    """Fake rows after Silver 8: (station, UTC time, timezone, temp_c).
    All other measurements are 1.0, unless temp_c is "EMPTY" (then all are empty)."""
    full = []
    for station, time, zone, temp in rows:
        values = [None] * len(MEASUREMENTS) if temp == "EMPTY" else [
            temp if m == "temp_c" else 1.0 for m in MEASUREMENTS]
        full.append((station, 2021, time, zone, *values))
    schema = ("station_code string, source_year int, observed_at_utc string, timezone string, "
              + ", ".join(f"{m} double" for m in MEASUREMENTS))
    return spark.createDataFrame(full, schema).withColumn(
        "observed_at_utc", F.to_timestamp("observed_at_utc"))


def files(spark):
    """bronze.inmet_files: SP01 in Sao Paulo, C891 in Antarctica (outside Brazil)."""
    return spark.createDataFrame([("SP01", 2021, "-23,5", "-46,6"),
                                  ("C891", 2021, "-62,1", "-58,4")],
                                 "station_code string, source_year int, latitude string, longitude string")


def reasons(spark, rows):
    good, quarantined = quarantine_rows(hours(spark, rows), files(spark))
    return good, [r["dq_issue"] for r in quarantined.collect()]


def test_good_row_stays(spark):
    good, found = reasons(spark, [("SP01", T, ZONE, 25.0)])
    assert good.count() == 1 and found == []
    assert "dq_issue" not in good.columns


@pytest.mark.parametrize("row, reason", [
    (("SP01", None, ZONE, 25.0), "bad_timestamp"),
    (("XX99", T, ZONE, 25.0), "unknown_station"),       # not in inmet_files
    (("SP01", T, None, 25.0), "unknown_timezone"),
    (("C891", T, ZONE, 25.0), "not_in_brazil"),
    (("SP01", T, ZONE, "EMPTY"), "all_values_empty"),
])
def test_each_reason(spark, row, reason):
    good, found = reasons(spark, [row])
    assert good.count() == 0 and found == [reason]


def test_one_empty_value_is_not_quarantined(spark):
    good, found = reasons(spark, [("SP01", T, ZONE, None)])    # only temp_c is empty
    assert good.count() == 1 and found == []


def test_first_reason_wins(spark):
    # bad time AND unknown station: we only write the first reason
    good, found = reasons(spark, [("XX99", None, ZONE, 25.0)])
    assert found == ["bad_timestamp"]


def test_no_row_is_lost_and_counts(spark):
    rows = [("SP01", T, ZONE, 25.0), ("SP01", None, ZONE, 25.0),
            ("XX99", T, ZONE, 25.0), ("C891", T, ZONE, 25.0)]
    good, quarantined = quarantine_rows(hours(spark, rows), files(spark))
    assert good.count() + quarantined.count() == len(rows)
    counts = {r["dq_issue"]: r["count"] for r in count_reasons(quarantined).collect()}
    assert counts == {"bad_timestamp": 1, "unknown_station": 1, "not_in_brazil": 1}
