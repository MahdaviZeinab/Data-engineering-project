"""
Tests for the Bronze job, with tiny fake INMET files (same layout, Latin-1).
Run:  python -m pytest -q
"""
from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from bronze.bronze_ingest import EXPECTED_HEADER, run_bronze

HOURLY = "bronze.inmet_hourly"
FILES = "bronze.inmet_files"
QUARANTINE = "bronze.inmet_quarantine"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # A small Spark for the tests. The tables go to a temporary folder.
    warehouse = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.warehouse.dir", str(warehouse))
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def empty_bronze(spark):
    # Every test starts without Bronze tables.
    spark.sql("DROP DATABASE IF EXISTS bronze CASCADE")


def station(code, name):
    # The 8 station lines at the top of an INMET file.
    return ["REGIAO:;SE", "UF:;SP", f"ESTACAO:;{name}", f"CODIGO (WMO):;{code}",
            "LATITUDE:;-23,5", "LONGITUDE:;-46,6", "ALTITUDE:;792,06", "DATA DE FUNDACAO:;25/07/06"]


def hours(days, year=2020):
    # Good hour lines, 24 per day, starting on 1 January.
    return [f"{date(year, 1, 1) + timedelta(d):%Y/%m/%d};{h:02d}00 UTC;0;" + "1,5;" * 16
            for d in range(days) for h in range(24)]


def write(folder, name, lines):
    # Write a fake INMET file in Latin-1, like INMET does.
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_bytes(("\n".join(lines) + "\n").encode("latin-1"))


@pytest.fixture()
def source(tmp_path):
    raw = tmp_path / "raw"
    header = EXPECTED_HEADER + ";"
    # complete: 2 days = 48 hours
    write(raw / "2020", "INMET_SE_SP_A001_SÃO PAULO_01-01-2020_A_02-01-2020.CSV",
          station("A001", "SÃO PAULO") + [header] + hours(2))
    # 40 of 48 hours, one hour twice and one broken line
    rows = hours(2)[:40]
    write(raw / "2020", "INMET_SE_SP_A002_CAMPINAS_01-01-2020_A_02-01-2020.CSV",
          station("A002", "CAMPINAS") + [header] + rows + [rows[-1], "2020/01/02;1700 UTC;0;1"])
    # a header that INMET never promised
    write(raw / "2020", "INMET_SE_SP_A003_SANTOS_01-01-2020_A_01-01-2020.CSV",
          station("A003", "SANTOS") + ["Date;Hour;Rain;"] + hours(1))
    # another year
    write(raw / "2021", "INMET_SE_SP_A001_SÃO PAULO_01-01-2021_A_01-01-2021.CSV",
          station("A001", "SÃO PAULO") + [header] + hours(1, 2021))
    return str(raw)


def files_by_station(spark, year=2020):
    rows = spark.table(FILES).where(f"source_year = {year}").collect()
    return {r["station_code"]: r for r in rows}


def test_tables_are_saved_in_spark(spark, source):
    run_bronze(spark, source)
    names = [t.name for t in spark.catalog.listTables("bronze")]
    assert sorted(names) == ["inmet_files", "inmet_hourly", "inmet_quarantine", "runs"]
    assert spark.table(HOURLY).count() == 48 + 41 + 24      # not the 24 rows of A003


def test_values_stay_raw_and_accents_survive(spark, source):
    run_bronze(spark, source)
    row = spark.table(HOURLY).where("station_code = 'A001' AND source_year = 2020") \
               .orderBy("source_line_number").first()
    assert row["source_line_number"] == 10
    assert row["source_file"] == "INMET_SE_SP_A001_SÃO PAULO_01-01-2020_A_02-01-2020.CSV"
    assert row["data"] == "2020/01/01" and row["hora_utc"] == "0000 UTC"
    assert row["temperatura_ar_c"] == "1,5"                               # still text
    assert row["_bronze_run_id"] is not None
    assert row["_bronze_ingestion_time"] is not None
    assert files_by_station(spark)["A001"]["station_name"] == "SÃO PAULO"


def test_files_table_counts_hours(spark, source):
    run_bronze(spark, source)
    files = files_by_station(spark)
    assert files["A001"]["expected_hours"] == 48 and files["A001"]["missing_hours"] == 0
    assert files["A002"]["received_hours"] == 40 and files["A002"]["missing_hours"] == 8
    assert files["A002"]["received_rows"] == 41                    # one hour is there twice
    assert files["A002"]["quarantined_lines"] == 1
    assert files["A001"]["header_ok"] is True
    assert files["A003"]["header_ok"] is False
    assert files["A003"]["received_hours"] == 0 and files["A003"]["quarantined_lines"] == 24


def test_broken_line_goes_to_quarantine(spark, source):
    run_bronze(spark, source)
    bad = spark.table(QUARANTINE).where("problem = 'wrong number of values'").collect()
    assert len(bad) == 1
    assert bad[0]["raw_line"] == "2020/01/02;1700 UTC;0;1"
    assert bad[0]["source_file"].startswith("INMET_SE_SP_A002")


def test_file_with_wrong_header_goes_to_quarantine(spark, source):
    run_bronze(spark, source)
    # none of its rows are in the hourly table, all 24 are in quarantine
    assert spark.table(HOURLY).where("station_code = 'A003'").count() == 0
    bad = spark.table(QUARANTINE).where("station_code = 'A003'")
    assert bad.count() == 24
    assert [r[0] for r in bad.select("problem").distinct().collect()] == ["unexpected header"]


def test_running_twice_gives_the_same_result(spark, source):
    run_bronze(spark, source, run_id="first")
    count = spark.table(HOURLY).count()
    run_bronze(spark, source, run_id="second")
    assert spark.table(HOURLY).count() == count
    run_ids = spark.table(HOURLY).select("_bronze_run_id").distinct().collect()
    assert [r[0] for r in run_ids] == ["second"]


def test_loading_one_year_keeps_the_other_years(spark, source):
    run_bronze(spark, source, run_id="all")
    run_bronze(spark, source, years=[2021], run_id="only_2021")
    runs = {r["source_year"]: r["_bronze_run_id"]
            for r in spark.table(HOURLY).select("source_year", "_bronze_run_id").distinct().collect()}
    assert runs == {2020: "all", 2021: "only_2021"}


def test_every_file_gets_a_status(spark, source):
    run_bronze(spark, source)
    status = {code: r["status"] for code, r in files_by_station(spark).items()}
    assert status == {"A001": "COMPLETE", "A002": "INCOMPLETE", "A003": "QUARANTINED"}


def test_every_run_is_in_the_runs_table(spark, source):
    run_bronze(spark, source, run_id="first")
    run_bronze(spark, source, years=[2021], run_id="second")
    runs = {r["run_id"]: r for r in spark.table("bronze.runs").collect()}
    assert set(runs) == {"first", "second"}
    assert runs["second"]["years"] == "2021" and runs["second"]["files"] == 1


def test_crash_while_writing_keeps_the_old_data(spark, source, monkeypatch):
    run_bronze(spark, source, run_id="good")

    # pretend the job crashes while it writes the LAST new table
    import bronze.bronze_ingest as bronze_ingest
    real = bronze_ingest.save_to_staging

    def crash(spark, table, name):
        if name == "bronze.inmet_files":
            raise RuntimeError("simulated crash")
        real(spark, table, name)

    monkeypatch.setattr(bronze_ingest, "save_to_staging", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        run_bronze(spark, source, run_id="bad")

    # all three tables still have the data of the "good" run
    for table in [HOURLY, FILES, QUARANTINE]:
        ids = [r[0] for r in spark.table(table).select("_bronze_run_id").distinct().collect()]
        assert ids == ["good"], table


def test_year_without_files_stops_and_keeps_the_old_data(spark, source, tmp_path):
    run_bronze(spark, source, run_id="good")
    (tmp_path / "raw" / "2022").mkdir()                      # an empty year folder
    (tmp_path / "raw" / "2022" / "readme.txt").write_text("not a CSV")
    with pytest.raises(Exception):
        run_bronze(spark, source, years=[2021, 2022], run_id="bad")
    ids = [r[0] for r in spark.table(HOURLY).select("_bronze_run_id").distinct().collect()]
    assert ids == ["good"]
