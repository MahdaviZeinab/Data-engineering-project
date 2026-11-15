"""Tests for the Silver job skeleton (S3), with a tiny fake Bronze table."""
import pytest
from pyspark.sql import SparkSession

from silver.contract import BRONZE_COLUMNS, MEASUREMENTS
from silver.silver_build import cast_values, count_not_numeric, make_time, run_silver

TEMP = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"   # becomes temp_c


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def fake_bronze(spark, output, columns):
    """Write 2 hourly rows (2020 and 2021) and 1 file row, all as text."""
    rows = [{c: "1" for c in columns} | {"source_year": y} for y in (2020, 2021)]
    hourly = spark.createDataFrame(rows)
    hourly.write.partitionBy("source_year").parquet(f"{output}/bronze/inmet_hourly")
    spark.createDataFrame([{"station_code": "1", "state": "SP", "source_year": 2020}]) \
         .write.partitionBy("source_year").parquet(f"{output}/bronze/inmet_files")


def test_runs_and_adds_run_id(spark, tmp_path):
    fake_bronze(spark, tmp_path, BRONZE_COLUMNS)
    df = run_silver(spark, str(tmp_path), run_id="test")
    assert df.count() == 2
    assert df.first()["_silver_run_id"] == "test"


def test_years_option(spark, tmp_path):
    fake_bronze(spark, tmp_path, BRONZE_COLUMNS)
    assert run_silver(spark, str(tmp_path), years=[2021]).count() == 1


def test_stops_on_wrong_schema(spark, tmp_path):
    fake_bronze(spark, tmp_path, [c for c in BRONZE_COLUMNS if c != "Data"])
    with pytest.raises(ValueError, match="Missing"):
        run_silver(spark, str(tmp_path))


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


# ---------------------------------------------------------------- Silver 5: time
def hours(spark, rows):
    """Make fake hour rows: station, date, hour."""
    return spark.createDataFrame(rows, "station_code string, Data string, `Hora UTC` string")


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
