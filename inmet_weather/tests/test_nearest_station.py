"""
Tests for the nearest station of every fire (silver/fire_station_matching.py).
Tiny fake tables, no Bronze needed.
Run:  python -m pytest -q tests/test_nearest_station.py
"""
import pytest
from pyspark.sql import SparkSession

from silver.fire_station_matching import add_nearest_station, distance_km, mark_no_station

HISTORY = "station_code string, source_year int, latitude double, longitude double, in_brazil boolean"
FIRES = "foco_id string, source_year int, lat double, lon double, dq_issue string"

# Two real stations: Brasilia (A001) and Goiania (A002), about 170 km apart.
BRASILIA = ("A001", 2020, -15.789, -47.926, True)
GOIANIA = ("A002", 2020, -16.642, -49.220, True)


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


def nearest(spark, fires, stations=(BRASILIA, GOIANIA)):
    # Run the step on fake fires and return {foco_id: row}.
    fires = spark.createDataFrame(fires, FIRES)
    history = spark.createDataFrame(list(stations), HISTORY)
    result = mark_no_station(add_nearest_station(fires, history))
    return {r["foco_id"]: r for r in result.collect()}


def test_distance_brasilia_goiania(spark):
    df = spark.createDataFrame([BRASILIA[2:4] + GOIANIA[2:4]], "a double, b double, c double, d double")
    km = df.select(distance_km("a", "b", "c", "d").alias("km")).first()["km"]
    assert 165 < km < 170                               # about 168 km on a map


def test_fire_next_to_a_station(spark):
    rows = nearest(spark, [("f1", 2020, -15.80, -47.93, None)])
    assert rows["f1"]["nearest_station_code"] == "A001"
    assert rows["f1"]["distance_km"] < 2
    assert rows["f1"]["dq_issue"] is None


def test_fire_between_two_stations_gets_the_closest(spark):
    # Closer to Goiania than to Brasilia, and in an other grid square than both.
    rows = nearest(spark, [("f1", 2020, -16.40, -48.80, None)])
    assert len(rows) == 1                               # still one row per fire
    assert rows["f1"]["nearest_station_code"] == "A002"
    assert 40 < rows["f1"]["distance_km"] < 60


def test_fire_with_no_station_nearby(spark):
    # A fire in Acre, far from both stations.
    rows = nearest(spark, [("f1", 2020, -9.0, -70.0, None)])
    assert rows["f1"]["nearest_station_code"] is None
    assert rows["f1"]["distance_km"] is None
    assert rows["f1"]["dq_issue"] == "no_station_nearby"


def test_only_stations_of_the_same_year_and_in_brazil(spark):
    # Both stations are at the fire, but neither may be used.
    moved = ("A003", 2020, -15.80, -47.93, False)        # in_brazil = false (bad coordinates)
    other_year = ("A004", 2019, -15.80, -47.93, True)    # only in the 2019 file
    rows = nearest(spark, [("f1", 2020, -15.80, -47.93, None)], stations=(moved, other_year))
    assert rows["f1"]["nearest_station_code"] is None


def test_other_problems_are_kept(spark):
    # A fire without coordinates keeps its first problem.
    rows = nearest(spark, [("f1", 2020, None, None, "bad_coordinates")])
    assert rows["f1"]["nearest_station_code"] is None
    assert rows["f1"]["dq_issue"] == "bad_coordinates"
