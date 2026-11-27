"""Tests for the Silver job skeleton (S3), with a tiny fake Bronze table."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.inmet_contract import BRONZE_COLUMNS, MEASUREMENTS
import json

from silver.inmet_build import (cast_values, count_conflicts, count_not_numeric, make_time,
                                 remove_duplicates, run_silver, write_run_summary)

TEMP = "temperatura_ar_c"   # becomes temp_c


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


FILE = {"state": "SP", "region": "SE", "station_name": "SAO PAULO",
        "latitude": "-23,5", "longitude": "-46,6", "end_date": "2021-12-31", "source_file": "f.csv"}


def fake_bronze(spark, columns):
    """Save 2 hourly rows (2020 and 2021) and 1 file row as Bronze Spark tables, all as text."""
    spark.catalog.clearCache()                       # Silver caches tables; forget them before dropping
    spark.sql("DROP DATABASE IF EXISTS bronze CASCADE")
    spark.sql("CREATE DATABASE bronze")
    rows = [{c: "1" for c in columns} | {"source_year": y} for y in (2020, 2021)]
    hourly = spark.createDataFrame(rows)
    hourly.write.partitionBy("source_year").saveAsTable("bronze.inmet_hourly")
    spark.createDataFrame([FILE | {"station_code": "1", "source_year": 2020}]) \
         .write.partitionBy("source_year").saveAsTable("bronze.inmet_files")


def good_bronze(spark):
    """Save 3 good Bronze rows for station A001 in 2021: 2 hours, one hour twice."""
    spark.catalog.clearCache()                       # Silver caches tables; forget them before dropping
    spark.sql("DROP DATABASE IF EXISTS bronze CASCADE")
    spark.sql("CREATE DATABASE bronze")
    base = {c: "20,5" for c in BRONZE_COLUMNS} | {
        "station_code": "A001", "data": "2021/01/01", "source_file": "f.csv",
        "source_line_number": "1", "_bronze_ingestion_time": "t1", "source_year": 2021}
    rows = [base | {"hora_utc": "0100 UTC"},
            base | {"hora_utc": "0100 UTC", "source_line_number": "2"},    # the same hour again
            base | {"hora_utc": "0200 UTC", "source_line_number": "3"}]
    spark.createDataFrame(rows).write.partitionBy("source_year").saveAsTable("bronze.inmet_hourly")
    spark.createDataFrame([FILE | {"station_code": "A001", "source_year": 2021}]) \
         .write.partitionBy("source_year").saveAsTable("bronze.inmet_files")


def test_runs_and_adds_run_id(spark):
    fake_bronze(spark, BRONZE_COLUMNS)
    # the fake rows have date "1" and hour "1", so both go to quarantine (bad_timestamp)
    good, quarantined, report = run_silver(spark, run_id="test")
    assert good.count() == 0
    assert [r["dq_issue"] for r in quarantined.collect()] == ["bad_timestamp"] * 2
    assert quarantined.first()["_silver_run_id"] == "test"


def test_years_option(spark):
    fake_bronze(spark, BRONZE_COLUMNS)
    good, quarantined, report = run_silver(spark, years=[2021])
    assert good.count() + quarantined.count() == 1


# whole run
def test_good_rows_get_station_details(spark):
    good_bronze(spark)
    good, quarantined, report = run_silver(spark, run_id="r1")
    rows = good.collect()
    assert len(rows) == 2                                  # the double hour is removed
    assert {(r.state, r.region, r.station_name) for r in rows} == {("SP", "SE", "SAO PAULO")}


def test_rerun_gives_the_same_checksum(spark):
    good_bronze(spark)
    first, second = {}, {}
    run_silver(spark, run_id="run1", summary=first)
    run_silver(spark, run_id="run2", summary=second)        # other run id, same data
    assert first["checksum"] == second["checksum"]
    assert first["reconcile"] == {2021: {"rows_read_from_bronze": 3, "good_rows_saved_in_silver": 2,
                                         "bad_rows_moved_to_quarantine": 0,
                                         "duplicate_rows_removed": 1}}


def test_quality_report_numbers(spark):
    good_bronze(spark)
    good, quarantined, report = run_silver(spark, run_id="r1")
    row = report.first().asDict()
    assert (row["source_year"], row["rows_read_from_bronze"], row["duplicate_rows_removed"],
            row["bad_rows_moved_to_quarantine"], row["good_rows_saved_in_silver"]) == (2021, 3, 1, 0, 2)
    assert row["empty_values_in_bronze"] == 0


def test_stops_on_wrong_schema(spark):
    fake_bronze(spark, [c for c in BRONZE_COLUMNS if c != "data"])
    with pytest.raises(ValueError, match="Missing"):
        run_silver(spark)


def temp_rows(spark, values):
    """One Bronze row per value; only the temperature column changes."""
    return spark.createDataFrame([{c: "1" for c in BRONZE_COLUMNS} | {TEMP: v} for v in values])


def test_cast_values(spark):
    df = cast_values(temp_rows(spark, ["884,1", ",8", "", None, "-9999", "25.5"]))
    assert [r["temp_c"] for r in df.collect()] == [884.1, 0.8, None, None, None, 25.5]


def test_comma_8_becomes_0_8(spark):
    assert cast_values(temp_rows(spark, [",8"])).first()["temp_c"] == 0.8


def test_cast_renames_to_english(spark):
    df = cast_values(temp_rows(spark, ["1"]))
    assert set(MEASUREMENTS) <= set(df.columns)
    assert TEMP not in df.columns
    assert dict(df.dtypes)["temp_c"] == "double"


def test_count_not_numeric(spark):
    counts = count_not_numeric(temp_rows(spark, ["abc", "1,2,3", "884,1", "", "-9999"]))
    assert counts["temp_c"] == 2       # only "abc" and "1,2,3"
    assert counts["precip_mm"] == 0


# time
def hours(spark, rows):
    """Make fake hour rows: station, date, hour."""
    return spark.createDataFrame(rows, "station_code string, data string, hora_utc string")


def stations(spark):
    return spark.createDataFrame([("AC01", "AC"), ("SP01", "SP"), ("XX01", "ZZ")],
                                 "station_code string, state string")


def test_new_year_acre_vs_sao_paulo(spark):
    # 04:00 UTC is still 31 Dec in Acre, but already 1 Jan in Sao Paulo
    df = make_time(hours(spark, [("AC01", "2021/01/01", "0400 UTC"),
                                 ("SP01", "2021/01/01", "0400 UTC")]), stations(spark))
    got = {r.station_code: (str(r.local_date), r.local_hour, r.timezone) for r in df.collect()}
    assert got["AC01"] == ("2020-12-31", 23, "America/Rio_Branco")
    assert got["SP01"] == ("2021-01-01", 1, "America/Sao_Paulo")


def test_bad_time_and_unknown_zone_get_a_reason(spark):
    df = make_time(hours(spark, [("SP01", "2021/01/01", "00:00"),       # other format, fine
                                 ("SP01", "2021/01/01", "2500 UTC"),    # wrong hour
                                 ("SP01", None, "0100 UTC"),            # date missing
                                 ("XX01", "2021/01/01", "0100 UTC")]),  # unknown state
                   stations(spark))
    issues = sorted((r.dq_issue or "ok") for r in df.collect())
    assert issues == ["bad_timestamp", "bad_timestamp", "ok", "unknown_timezone"]


# duplicates
def dup_rows(spark, rows):
    """Fake rows after Silver 5: (station, UTC time, source_year, temp_c, ingestion time).
    All other measurements are 1.0, so only temp_c decides 'fewer missing values'.
    Each row gets a file name and a line number (row 1 -> line 1, row 2 -> line 2 ...)."""
    others = [m for m in MEASUREMENTS if m != "temp_c"]
    full = [(*row, "f.csv", line, *[1.0] * len(others)) for line, row in enumerate(rows, 1)]
    schema = ("station_code string, observed_at_utc string, source_year int, temp_c double, "
              "_bronze_ingestion_time string, source_file string, source_line_number int, "
              + ", ".join(f"{m} double" for m in others))
    return spark.createDataFrame(full, schema).withColumn(
        "observed_at_utc", F.to_timestamp("observed_at_utc"))


H = "2021-01-01 05:00:00"


def test_one_row_per_station_and_hour(spark):
    df = remove_duplicates(dup_rows(spark, [("A1", H, 2021, 25.0, "t1"),
                                            ("A1", H, 2021, 25.0, "t1"),
                                            ("B1", H, 2021, 25.0, "t1")]))   # other station
    assert df.count() == 2


def test_keeps_fewer_missing_values(spark):
    df = remove_duplicates(dup_rows(spark, [("A1", H, 2021, None, "t2"),
                                            ("A1", H, 2021, 25.0, "t1")]))
    assert df.first()["temp_c"] == 25.0


def test_keeps_right_year_first(spark):
    # the row from the 2020 file is wrong, even with fewer missing values
    df = remove_duplicates(dup_rows(spark, [("A1", H, 2020, 25.0, "t2"),
                                            ("A1", H, 2021, None, "t1")]))
    assert df.first()["source_year"] == 2021


def test_keeps_newest_record(spark):
    df = remove_duplicates(dup_rows(spark, [("A1", H, 2021, 20.0, "2024-01-01"),
                                            ("A1", H, 2021, 30.0, "2025-01-01")]))
    assert df.first()["temp_c"] == 30.0


def test_rows_without_time_are_kept(spark):
    df = remove_duplicates(dup_rows(spark, [("A1", None, 2021, 25.0, "t1"),
                                            ("A1", None, 2021, 25.0, "t1")]))
    assert df.count() == 2              # Silver 9 deals with them


def test_same_rows_keep_first_line_every_time(spark):
    # two rows that are equal in everything else: the lower line number wins, always
    for _ in range(3):
        df = remove_duplicates(dup_rows(spark, [("A1", H, 2021, 20.0, "t1"),
                                                ("A1", H, 2021, 30.0, "t1")]))
        row = df.first()
        assert (row["source_line_number"], row["temp_c"]) == (1, 20.0)


def test_count_conflicts(spark):
    df = dup_rows(spark, [("A1", H, 2021, 25.0, "t1"),     # A1: same values -> no conflict
                          ("A1", H, 2021, 25.0, "t2"),
                          ("B1", H, 2021, 20.0, "t1"),     # B1: 20 vs 30 -> conflict
                          ("B1", H, 2021, 30.0, "t1"),
                          ("C1", H, 2021, None, "t1"),     # C1: empty vs 25 -> conflict
                          ("C1", H, 2021, 25.0, "t1"),
                          ("D1", H, 2021, 25.0, "t1")])    # D1: only once
    assert count_conflicts(df) == 2


# run summary
def test_run_fills_summary(spark):
    fake_bronze(spark, BRONZE_COLUMNS)
    summary = {}
    run_silver(spark, run_id="test", summary=summary)
    assert summary["bronze_rows"] == 2
    assert {"not_numeric_values", "duplicates_removed", "duplicate_conflicts"} <= set(summary)


def test_write_run_summary(spark, tmp_path):
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.createDataFrame([(2021,), (2021,)], "source_year int") \
         .write.mode("overwrite").saveAsTable("silver.inmet_hourly")
    spark.createDataFrame([(2021, "bad_timestamp")], "source_year int, dq_issue string") \
         .write.mode("overwrite").saveAsTable("silver.inmet_quarantine")
    path = write_run_summary(str(tmp_path), "r1", [2021], {"duplicates_removed": 3}, spark)
    saved = json.loads(path.read_text())
    assert path.name == "silver_r1.json"
    assert saved["duplicates_removed"] == 3
    assert saved["silver_rows_per_year"] == {"2021": 2}
    assert saved["quarantined_per_reason"] == {"bad_timestamp": 1}
