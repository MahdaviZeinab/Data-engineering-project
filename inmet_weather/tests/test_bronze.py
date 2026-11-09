"""
Tests for the Bronze job, with tiny fake INMET files (same layout, Latin-1).
Run:  python -m pytest -q
"""
from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from bronze.bronze_ingest import EXPECTED_HEADER, replace_years, run_bronze


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def station(code, name):
    return ["REGIAO:;SE", "UF:;SP", f"ESTACAO:;{name}", f"CODIGO (WMO):;{code}",
            "LATITUDE:;-23,5", "LONGITUDE:;-46,6", "ALTITUDE:;792,06", "DATA DE FUNDACAO:;25/07/06"]


def hours(days):
    return [f"{date(2020, 1, 1) + timedelta(d):%Y/%m/%d};{h:02d}00 UTC;0;" + "1,5;" * 16
            for d in range(days) for h in range(24)]


def write(folder, name, lines):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_bytes(("\n".join(lines) + "\n").encode("latin-1"))


@pytest.fixture()
def source(tmp_path):
    y = tmp_path / "inmet_data" / "2020"
    header = EXPECTED_HEADER + ";"
    # complete: 2 days = 48 hours
    write(y, "INMET_SE_SP_A001_SAO PAULO_01-01-2020_A_02-01-2020.CSV",
          station("A001", "SÃO PAULO") + [header] + hours(2))
    # 40 of 48 hours, one hour twice, one broken line
    rows = hours(2)[:40]
    write(y, "INMET_SE_SP_A002_CAMPINAS_01-01-2020_A_02-01-2020.CSV",
          station("A002", "CAMPINAS") + [header] + rows + [rows[-1], "2020/01/02;1700 UTC;0;1"])
    # name says 1 Jan only, but the file has 2 days and misses 2 hours of 1 Jan
    # (like the real B827 file): extra hours must NOT hide the missing ones
    write(y, "INMET_SE_SP_A004_SOROCABA_01-01-2020_A_01-01-2020.CSV",
          station("A004", "SOROCABA") + [header] + hours(2)[2:])
    # header that INMET never promised
    write(y, "INMET_SE_SP_A003_SANTOS_01-01-2020_A_01-01-2020.CSV",
          station("A003", "SANTOS") + ["Date;Hour;Rain;"] + hours(1))
    return tmp_path / "inmet_data"


def read(spark, out, table):
    return spark.read.parquet(f"{out}/bronze/{table}")


def test_delivery_check_and_quarantine(spark, source, tmp_path):
    out = str(tmp_path / "out")
    report = run_bronze(spark, str(source), out)
    files = {r["station_code"]: r for r in read(spark, out, "inmet_files").collect()}

    assert report["files"] == 4
    assert files["A001"]["status"] == "COMPLETE" and files["A001"]["received_rows"] == 48
    assert files["A002"]["status"] == "INCOMPLETE"
    assert files["A002"]["missing_hours"] == 8
    assert files["A002"]["duplicate_rows"] == 1
    assert files["A002"]["quarantined_lines"] == 1
    assert files["A004"]["status"] == "INCOMPLETE"
    assert files["A004"]["missing_hours"] == 2 and files["A004"]["extra_hours"] == 24
    assert files["A001"]["extra_hours"] == 0
    assert files["A003"]["status"] == "QUARANTINED"
    assert read(spark, out, "inmet_hourly").where("station_code = 'A003'").count() == 0
    assert read(spark, out, "inmet_quarantine").where("problem = 'unexpected_header'").count() == 24


def test_values_stay_raw_and_accents_survive(spark, source, tmp_path):
    out = str(tmp_path / "out")
    run_bronze(spark, str(source), out)
    row = read(spark, out, "inmet_hourly").where("station_code = 'A001'") \
        .orderBy("source_line_number").first()
    assert row["source_line_number"] == 10
    assert row["Data"] == "2020/01/01" and row["Hora UTC"] == "0000 UTC"
    assert row["TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"] == "1,5"   # still text
    assert "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)" in row.asDict()            # original name kept
    names = {r["station_code"]: r["station_name"] for r in read(spark, out, "inmet_files").collect()}
    assert names["A001"] == "SÃO PAULO"


def test_running_twice_gives_the_same_result(spark, source, tmp_path):
    out = str(tmp_path / "out")
    run_bronze(spark, str(source), out, run_id="first")
    count = read(spark, out, "inmet_hourly").count()
    run_bronze(spark, str(source), out, run_id="second")
    hourly = read(spark, out, "inmet_hourly")
    assert hourly.count() == count
    assert [r[0] for r in hourly.select("_bronze_run_id").distinct().collect()] == ["second"]


def test_failed_write_keeps_the_old_data(spark, source, tmp_path):
    out = str(tmp_path / "out")
    run_bronze(spark, str(source), out, run_id="good")
    before = read(spark, out, "inmet_hourly").count()

    # Pretend the new write came out wrong: the row count does not match.
    new = read(spark, out, "inmet_hourly").limit(5)
    with pytest.raises(RuntimeError, match="Nothing was replaced"):
        replace_years(spark, new, f"{out}/bronze/inmet_hourly", [2020], before, "bad")

    after = read(spark, out, "inmet_hourly")
    assert after.count() == before                                    # old data untouched
    assert [r[0] for r in after.select("_bronze_run_id").distinct().collect()] == ["good"]


def test_no_rows_are_lost(spark, tmp_path):
    y = tmp_path / "src" / "2020"
    header = EXPECTED_HEADER + ";"

    # 1) a normal file
    write(y, "INMET_SE_SP_A001_SAO PAULO_01-01-2020_A_01-01-2020.CSV",
          station("A001", "X") + [header] + hours(1))
    # 2) a file with a bad name
    write(y, "broken_name.CSV",
          station("A009", "Y") + [header] + hours(1))
    # 3) a file in the 2020 folder whose name starts in 2019
    write(y, "INMET_SE_SP_A005_ITU_31-12-2019_A_01-01-2020.CSV",
          station("A005", "ITU") + [header] + hours(1))

    out = str(tmp_path / "out")
    report = run_bronze(spark, str(tmp_path / "src"), out, years=[2020])

    # what is on disk must match the report
    assert read(spark, out, "inmet_files").count() == report["files"]
    assert read(spark, out, "inmet_hourly").count() == report["received_rows"]
    assert read(spark, out, "inmet_quarantine").count() == report["quarantined_lines"]

    

def test_one_failed_table_replaces_no_table(spark, source, tmp_path, monkeypatch):
    out = str(tmp_path / "out")
    run_bronze(spark, str(source), out, run_id="good")

    # make the check of the LAST table (inmet_files) fail in the next run
    import bronze.bronze_ingest as bronze_ingest
    real = bronze_ingest.write_temp

    def failing(spark, df, path, years, expected_rows, run_id):
        if path.endswith("inmet_files"):
            raise RuntimeError("simulated failure")
        return real(spark, df, path, years, expected_rows, run_id)

    monkeypatch.setattr(bronze_ingest, "write_temp", failing)
    with pytest.raises(RuntimeError, match="simulated failure"):
        run_bronze(spark, str(source), out, run_id="bad")

    # all three tables still come from the "good" run
    for table in ["inmet_hourly", "inmet_quarantine", "inmet_files"]:
        ids = [r[0] for r in read(spark, out, table).select("_bronze_run_id").distinct().collect()]
        assert ids == ["good"], table