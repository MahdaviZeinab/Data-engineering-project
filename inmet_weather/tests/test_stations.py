"""Tests for Silver 7: station information, with a tiny fake bronze.inmet_files table."""
import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver import contract
from silver.silver_build import save_silver
from silver.stations import build_stations, latest_station, station_history

SCHEMA = ("source_file string, source_year int, station_code string, end_date date, "
          "region string, state string, station_name string, wmo_code string, "
          "latitude string, longitude string, altitude string, foundation_date string, "
          "_bronze_run_id string")

# Brasilia, as INMET writes it (',' as decimal sign)
BSB = dict(region="CO", state="DF", name="BRASILIA", lat="-15,78944444",
           lon="-47,92583332", alt="1159,54", founded="2000-05-07")


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


def files(spark, rows):
    """rows: (station_code, year, changes to BSB). One fake file per row."""
    data = []
    for code, year, change in rows:
        info = BSB | change
        data.append((f"INMET_{code}_{year}.CSV", year, code, dt.date(year, 12, 31),
                     info["region"], info["state"], info["name"], "A001",
                     info["lat"], info["lon"], info["alt"], info["founded"], "run1"))
    return spark.createDataFrame(data, SCHEMA)


def by_year(df):
    return {r.source_year: r for r in df.collect()}


# ---------------------------------------------------------------- types
def test_numbers_and_date_are_clean(spark):
    r = station_history(files(spark, [("A001", 2021, {})])).first()
    assert (r.latitude, r.longitude, r.altitude) == (-15.78944444, -47.92583332, 1159.54)
    assert r.foundation_date == dt.date(2000, 5, 7)


def test_old_date_formats(spark):
    df = station_history(files(spark, [("A001", 2019, {"founded": "07/05/00"}),
                                       ("A001", 2020, {"founded": "07/05/2000"}),
                                       ("A001", 2021, {"founded": "-9999"})]))
    got = {y: r.foundation_date for y, r in by_year(df).items()}
    assert got == {2019: dt.date(2000, 5, 7), 2020: dt.date(2000, 5, 7), 2021: None}


# ---------------------------------------------------------------- history
def test_one_row_per_station_and_year(spark):
    df = station_history(files(spark, [("A001", 2020, {}), ("A001", 2021, {}),
                                       ("A002", 2021, {}), ("", 2021, {})]))   # bad file name
    assert sorted((r.station_code, r.source_year) for r in df.collect()) == \
        [("A001", 2020), ("A001", 2021), ("A002", 2021)]


def test_two_files_same_year_keeps_the_last_one(spark):
    rows = [("INMET_A001_a.CSV", 2021, "A001", dt.date(2021, 6, 30), *["x"] * 9),
            ("INMET_A001_b.CSV", 2021, "A001", dt.date(2021, 12, 31), *["x"] * 9)]
    df = station_history(spark.createDataFrame(rows, SCHEMA))
    assert df.count() == 1
    assert df.first().source_file == "INMET_A001_b.CSV"


# ---------------------------------------------------------------- changes
def test_name_change(spark):
    df = station_history(files(spark, [("A001", 2020, {}),
                                       ("A001", 2021, {"name": "BRASILIA NOVA"}),
                                       ("A001", 2022, {"name": "brasilia nova "})]))  # case/space only
    got = {y: r.name_changed for y, r in by_year(df).items()}
    assert got == {2020: False, 2021: True, 2022: False}


def test_location_change(spark):
    df = station_history(files(spark, [("A001", 2020, {}),
                                       ("A001", 2021, {"lat": "-15,7894"}),     # rounding only
                                       ("A001", 2022, {"lat": "-15,90"}),       # moved ~12 km
                                       ("A001", 2023, {"lat": "-15,90", "alt": "1170"})]))  # higher
    got = {y: r.location_changed for y, r in by_year(df).items()}
    assert got == {2020: False, 2021: False, 2022: True, 2023: True}


def test_missing_coordinates_are_not_a_change(spark):
    df = station_history(files(spark, [("A001", 2020, {}), ("A001", 2021, {"lat": ""})]))
    assert by_year(df)[2021].location_changed is False


# ---------------------------------------------------------------- time zone
def test_timezone_from_state_and_override(spark, monkeypatch):
    monkeypatch.setattr(contract, "STATION_TIMEZONE_OVERRIDE", {"A003": "America/Noronha"})
    df = station_history(files(spark, [("A001", 2021, {}),
                                       ("A002", 2021, {"state": "ac"}),     # lower case
                                       ("A003", 2021, {"state": "PE"}),
                                       ("A004", 2021, {"state": "ZZ"})]))
    got = {r.station_code: r.timezone for r in df.collect()}
    assert got == {"A001": "America/Sao_Paulo", "A002": "America/Rio_Branco",
                   "A003": "America/Noronha", "A004": None}


# ---------------------------------------------------------------- inside Brazil
def test_inside_brazil(spark):
    df = station_history(files(spark, [
        ("A001", 2021, {}),                                         # Brasilia
        ("A002", 2021, {"lat": "-3,85", "lon": "-32,42"}),          # Fernando de Noronha
        ("A003", 2021, {"lat": "15,78944444"}),                     # minus sign lost
        ("A004", 2021, {"lat": "38,72", "lon": "-9,14"}),           # Lisbon
        ("A005", 2021, {"lat": "-9999", "lon": ""}),                # no coordinates
    ]))
    got = {r.station_code: r.in_brazil for r in df.collect()}
    assert got == {"A001": True, "A002": True, "A003": False, "A004": False, "A005": False}


# ---------------------------------------------------------------- latest
def test_latest_station(spark):
    history = station_history(files(spark, [("A001", 2020, {}),
                                            ("A001", 2021, {"name": "BRASILIA NOVA"}),
                                            ("A001", 2022, {"name": "BRASILIA NOVA", "lat": "-15,90"}),
                                            ("A002", 2021, {})]))
    latest = {r.station_code: r for r in latest_station(history).collect()}
    a = latest["A001"]
    assert len(latest) == 2
    assert (a.station_name, a.latitude) == ("BRASILIA NOVA", -15.90)   # newest year wins
    assert (a.first_year, a.last_year, a.years) == (2020, 2022, 3)
    assert (a.name_changes, a.location_changes) == (1, 1)
    assert (latest["A002"].name_changes, latest["A002"].location_changes) == (0, 0)


# ---------------------------------------------------------------- whole step
def test_build_stations_saves_both_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    files(spark, [("A001", 2020, {}), ("A001", 2021, {}), ("A002", 2021, {})]) \
        .write.mode("overwrite").partitionBy("source_year").saveAsTable("bronze.inmet_files")
    build_stations(spark, "test")
    history = spark.table("silver.station_history")
    station = spark.table("silver.station")
    assert history.count() == 3
    assert station.count() == 2
    assert station.first()["_silver_run_id"] == "test"


# ---------------------------------------------------------------- saving all Silver tables together
def silver_hourly(spark, run_id, years):
    """A tiny clean Silver hourly table: 1 row per year."""
    rows = [("A001", f"{year}-01-01 00:00:00", year, run_id) for year in years]
    df = spark.createDataFrame(rows, "station_code string, observed_at_utc string, "
                                     "source_year int, _silver_run_id string")
    for m in contract.MEASUREMENTS:
        df = df.withColumn(m, F.lit(1.0))
    return df


def test_crash_while_saving_silver_keeps_all_old_tables(spark, monkeypatch):
    spark.catalog.clearCache()                       # Silver caches tables; forget them before dropping
    spark.sql("DROP DATABASE IF EXISTS silver CASCADE")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    files(spark, [("A001", 2020, {}), ("A001", 2021, {})]) \
        .write.mode("overwrite").partitionBy("source_year").saveAsTable("bronze.inmet_files")
    save_silver(spark, silver_hourly(spark, "good", [2020, 2021]), [2020, 2021], "good")

    # pretend the job crashes AFTER silver.inmet_hourly was already replaced
    import common.spark as common_spark
    real = common_spark.move_from_staging

    def crash(spark, name, years=None):
        if name == "silver.station":
            raise RuntimeError("simulated crash")
        real(spark, name, years)

    monkeypatch.setattr(common_spark, "move_from_staging", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        save_silver(spark, silver_hourly(spark, "bad", [2021]), [2021], "bad")

    # all three Silver tables have the "good" run again, and no year was lost
    for table in ["silver.inmet_hourly", "silver.station_history", "silver.station"]:
        ids = [r[0] for r in spark.table(table).select("_silver_run_id").distinct().collect()]
        assert ids == ["good"], table
    assert spark.table("silver.inmet_hourly").count() == 2


def test_save_silver_also_saves_quarantine(spark):
    # Silver 9: the quarantined rows are saved together with the other Silver tables
    spark.catalog.clearCache()                       # Silver caches tables; forget them before dropping
    spark.sql("DROP DATABASE IF EXISTS silver CASCADE")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    files(spark, [("A001", 2021, {})]) \
        .write.mode("overwrite").partitionBy("source_year").saveAsTable("bronze.inmet_files")
    quarantined = spark.createDataFrame([("A001", 2021, "bad_timestamp", "run1")],
                                        "station_code string, source_year int, "
                                        "dq_issue string, _silver_run_id string")
    save_silver(spark, silver_hourly(spark, "run1", [2021]), [2021], "run1", quarantined)
    reasons = [r.dq_issue for r in spark.table("silver.inmet_quarantine").collect()]
    assert reasons == ["bad_timestamp"]
