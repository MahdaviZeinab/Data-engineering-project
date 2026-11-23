"""
Tests for the state check of the fires (silver/state_check.py).
Run:  python -m pytest -q tests/test_state_check.py
"""
import pytest
from pyspark.sql import SparkSession

from silver import fire_contract
from silver.contract import STATE_TIMEZONE
from silver.state_check import inside_state


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def check(spark, state, lat, lon):
    df = spark.createDataFrame([(state, lat, lon)], "state string, lat double, lon double")
    return df.select(inside_state().alias("ok")).first()["ok"]


def test_every_state_has_a_box():
    assert set(fire_contract.STATE_BOX) == set(STATE_TIMEZONE)
    for lat_min, lat_max, lon_min, lon_max in fire_contract.STATE_BOX.values():
        assert lat_min < lat_max and lon_min < lon_max


def test_fire_in_its_own_state(spark):
    assert check(spark, "DF", -15.79, -47.93) is True          # Brasília
    assert check(spark, "MT", -10.51, -53.48) is True


def test_fire_in_an_other_state(spark):
    assert check(spark, "SP", -10.51, -53.48) is False         # this point is in Mato Grosso
    assert check(spark, "MT", -53.48, -10.51) is False         # lat and lon swapped


def test_fire_just_over_the_border_is_ok(spark):
    # DF ends at lon -47.30; 4 km further east still counts as DF (the margin).
    assert check(spark, "DF", -15.79, -47.26) is True


def test_unknown_state_gives_no_answer(spark):
    assert check(spark, None, -15.79, -47.93) is None          # that is "unknown_state"
