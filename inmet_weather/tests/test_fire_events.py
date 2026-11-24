"""
Tests for merging the same fire seen twice (silver/fire_events.py).
Run:  python -m pytest -q tests/test_fire_events.py
"""
import datetime as dt

import pytest
from pyspark.sql import SparkSession

from silver.fire_events import add_fire_events

FIRES = "foco_id string, observed_at_utc timestamp, local_date date, lat double, lon double"
DAY = dt.date(2020, 8, 14)
NOON = dt.datetime(2020, 8, 14, 17, 0)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def events(spark, fires):
    # Run the step on fake fire points and return {foco_id: row}.
    df = add_fire_events(spark.createDataFrame(fires, FIRES))
    return {r["foco_id"]: r for r in df.collect()}


def test_two_points_of_one_fire_are_one_event(spark):
    # 2 points about 300 m apart, seen the same day: the same fire.
    rows = events(spark, [("a", NOON, DAY, -10.512, -53.481),
                          ("b", NOON + dt.timedelta(minutes=1), DAY, -10.514, -53.483)])
    assert rows["a"]["fire_event_id"] == rows["b"]["fire_event_id"] == "a"   # "a" was seen first
    assert rows["a"]["points_in_event"] == 2
    assert (rows["a"]["first_in_event"], rows["b"]["first_in_event"]) == (True, False)


def test_far_apart_is_two_events(spark):
    rows = events(spark, [("a", NOON, DAY, -10.51, -53.48),
                          ("b", NOON, DAY, -10.61, -53.48)])         # 11 km to the south
    assert rows["a"]["fire_event_id"] != rows["b"]["fire_event_id"]
    assert rows["b"]["first_in_event"] is True


def test_same_place_next_day_is_a_new_event(spark):
    rows = events(spark, [("a", NOON, DAY, -10.512, -53.481),
                          ("b", NOON + dt.timedelta(days=1), DAY + dt.timedelta(days=1),
                           -10.512, -53.481)])
    assert rows["a"]["fire_event_id"] == "a"
    assert rows["b"]["fire_event_id"] == "b"
