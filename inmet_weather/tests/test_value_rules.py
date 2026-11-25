"""Tests for Silver 8: invalid values (-> empty) and unusual values (-> flag only)."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.inmet_contract import MEASUREMENTS
from silver.inmet_value_rules import apply_value_rules, count_flags


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# One normal hour: nothing in it is invalid or unusual.
NORMAL = {
    "precip_mm": 0.0, "pressure_mb": 900.0, "pressure_max_mb": 901.0, "pressure_min_mb": 899.0,
    "radiation_kj_m2": 1000.0, "temp_c": 25.0, "dew_point_c": 20.0,
    "temp_max_c": 26.0, "temp_min_c": 24.0, "dew_point_max_c": 21.0, "dew_point_min_c": 19.0,
    "humidity_max_pct": 75.0, "humidity_min_pct": 65.0, "humidity_pct": 70.0,
    "wind_dir_deg": 180.0, "wind_gust_ms": 5.0, "wind_speed_ms": 2.0,
}


def hours(spark, *changes, times=None):
    """One row per dict in changes: a NORMAL hour with some values changed.
    All rows are station A1 in 2021, one hour apart (or at the given times)."""
    times = times or [f"2021-01-01 {h:02d}:00:00" for h in range(len(changes))]
    rows = [("A1", t, 2021, *[(NORMAL | c)[m] for m in MEASUREMENTS])
            for t, c in zip(times, changes)]
    schema = ("station_code string, observed_at_utc string, source_year int, "
              + ", ".join(f"{m} double" for m in MEASUREMENTS))
    df = spark.createDataFrame(rows, schema)
    return df.withColumn("observed_at_utc", F.to_timestamp("observed_at_utc"))


def one(spark, change):
    """Run the rules on one changed hour and return that row."""
    return apply_value_rules(hours(spark, change)).first()


# normal and empty values
def test_normal_hour_has_no_flags(spark):
    row = one(spark, {})
    assert row["dq_flags"] == []
    assert row["temp_c"] == 25.0


def test_empty_value_is_not_flagged(spark):
    row = one(spark, {"humidity_pct": None, "precip_mm": None})
    assert row["dq_flags"] == []


# invalid -> empty
def test_humidity_130_becomes_empty(spark):
    row = one(spark, {"humidity_pct": 130.0})
    assert row["humidity_pct"] is None
    assert row["dq_flags"] == ["invalid_humidity_pct"]
    assert row["temp_c"] == 25.0                     # the rest of the row stays


def test_negative_rain_becomes_empty(spark):
    row = one(spark, {"precip_mm": -1.0})
    assert row["precip_mm"] is None
    assert "invalid_precip_mm" in row["dq_flags"]


def test_max_below_min_empties_both(spark):
    row = one(spark, {"temp_max_c": 20.0, "temp_min_c": 24.0})
    assert row["temp_max_c"] is None and row["temp_min_c"] is None
    assert sorted(row["dq_flags"]) == ["invalid_temp_max_c", "invalid_temp_min_c"]


def test_dew_point_above_temperature(spark):
    assert one(spark, {"dew_point_c": 25.4})["dew_point_c"] == 25.4     # inside the 0.5 room
    row = one(spark, {"dew_point_c": 30.0})
    assert row["dew_point_c"] is None
    assert row["dq_flags"] == ["invalid_dew_point_c"]


def test_too_much_radiation(spark):
    row = one(spark, {"radiation_kj_m2": 6000.0})
    assert row["radiation_kj_m2"] is None
    assert row["dq_flags"] == ["invalid_radiation_kj_m2"]


def test_negative_radiation_becomes_0(spark):
    row = one(spark, {"radiation_kj_m2": -3.0})
    assert row["radiation_kj_m2"] == 0.0
    assert row["dq_flags"] == ["radiation_negative_set_to_0"]


# unusual -> keep + flag
def test_rain_120_is_kept_with_a_flag(spark):
    row = one(spark, {"precip_mm": 120.0})
    assert row["precip_mm"] == 120.0
    assert row["dq_flags"] == ["unusual_precip_mm"]


def test_invalid_value_is_not_also_unusual(spark):
    # pressure 0 is invalid -> empty; it should not also get "unusual" (below 700)
    row = one(spark, {"pressure_mb": 0.0})
    assert row["dq_flags"] == ["invalid_pressure_mb"]


def test_temperature_jump(spark):
    df = apply_value_rules(hours(spark, {"temp_c": 20.0, "dew_point_c": 15.0},
                                        {"temp_c": 32.0, "dew_point_c": 15.0}))
    flags = [r["dq_flags"] for r in df.orderBy("observed_at_utc").collect()]
    assert flags == [[], ["unusual_temp_jump"]]


def test_no_jump_when_hours_are_not_next_to_each_other(spark):
    df = apply_value_rules(hours(spark, {"temp_c": 20.0, "dew_point_c": 15.0},
                                        {"temp_c": 32.0, "dew_point_c": 15.0},
                                 times=["2021-01-01 00:00:00", "2021-01-01 05:00:00"]))
    assert all(r["dq_flags"] == [] for r in df.collect())


# counts
def test_count_flags(spark):
    df = apply_value_rules(hours(spark, {"humidity_pct": 130.0}, {"humidity_pct": 120.0},
                                        {"precip_mm": 120.0}, {}))
    counts = {r["flag"]: r["count"] for r in count_flags(df).collect()}
    assert counts == {"invalid_humidity_pct": 2, "unusual_precip_mm": 1}
