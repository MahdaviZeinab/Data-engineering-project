"""
Tests for the fire data (INPE focos de queimadas): download, Bronze and Silver.
Tiny fake files with the same layout as the real ones, no network.
Run:  python -m pytest -q tests/test_fire.py
"""
import zipfile

import pytest
from pyspark.sql import SparkSession

from bronze import injection
from bronze.fire_ingest import EXPECTED_HEADER, run_fire_bronze
from silver import fire_contract
from silver.contract import STATE_TIMEZONE, check_schema
from silver.fire_build import run_fire_silver, save_fire_silver

FOCI = "bronze.fire_foci"
FILES = "bronze.fire_files"
QUARANTINE = "bronze.fire_quarantine"


# ---------------------------------------------------------------------------
# Contract (plain Python)
# ---------------------------------------------------------------------------
def test_every_state_maps_to_a_known_uf():
    assert len(fire_contract.STATE_TO_UF) == 27                     # 26 states + DF
    assert set(fire_contract.STATE_TO_UF.values()) == set(STATE_TIMEZONE)


def test_fire_schema_check():
    check_schema(fire_contract.BRONZE_COLUMNS, fire_contract.BRONZE_COLUMNS)   # no error
    with pytest.raises(ValueError, match="Missing: \\['bioma'\\]"):
        check_schema([c for c in fire_contract.BRONZE_COLUMNS if c != "bioma"],
                     fire_contract.BRONZE_COLUMNS)


# ---------------------------------------------------------------------------
# Download step: the same injection.py as INMET, with --source fire (no network)
# ---------------------------------------------------------------------------
@pytest.fixture()
def raw_fire(tmp_path, monkeypatch):
    # The fake data/raw folder, with the source set to "fire".
    monkeypatch.setattr(injection, "RAW", tmp_path)
    monkeypatch.setattr(injection, "SOURCE", "fire")
    monkeypatch.setattr(injection, "download", lambda year: True)   # never go online
    return tmp_path


def test_fire_url():
    url = injection.SOURCES["fire"]
    assert url.format(year=2020).endswith("/focos_br_ref_2020.zip")


def test_fire_files_go_next_to_the_inmet_files(raw_fire):
    with zipfile.ZipFile(raw_fire / "fire_2020.zip", "w") as archive:
        archive.writestr("focos_br_ref_2020.csv", "a")
    assert injection.land(2020)
    # data/raw/2020/fire/ has the CSV and _DONE; the zip stays in data/raw
    assert sorted(p.name for p in (raw_fire / "2020" / "fire").iterdir()) == ["_DONE", "focos_br_ref_2020.csv"]
    assert (raw_fire / "fire_2020.zip").exists()
    assert injection.is_unzipped(2020)


# ---------------------------------------------------------------------------
# Bronze and Silver (Spark)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def spark(tmp_path_factory):
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
def empty_lake(spark):
    spark.catalog.clearCache()                       # Silver caches tables; forget them before dropping
    spark.sql("DROP DATABASE IF EXISTS bronze CASCADE")
    spark.sql("DROP DATABASE IF EXISTS silver CASCADE")


def line(foco_id, state="MATO GROSSO", lat="  -10.513000 ", time="2020-06-16 17:00:00",
         biome="Amazônia"):
    # One fire line, with the extra spaces INPE writes around the numbers.
    return f" 14228 ,{foco_id},{lat},  -53.483000 ,{time},Brasil,{state},PEIXOTO,{biome}"


def write(source, year, lines, header=EXPECTED_HEADER):
    folder = source / str(year) / "fire"           # data/raw/<year>/fire/
    folder.mkdir(parents=True, exist_ok=True)
    text = "\n".join([header] + lines) + "\n"
    (folder / f"focos_br_ref_{year}.csv").write_text(text, encoding="utf-8")


def test_bronze_keeps_text_and_quarantines_bad_lines(spark, tmp_path):
    write(tmp_path, 2020, [line("a"), line("b"), line("b"), "too,few,values"])
    run_fire_bronze(spark, str(tmp_path), [2020], run_id="first")

    foci = spark.table(FOCI)
    assert foci.count() == 3                        # the double "b" stays in Bronze
    first = foci.where("foco_id = 'a'").first()
    assert first["lat"] == "  -10.513000 "          # Bronze does not change the values
    assert first["source_line_number"] == 2
    assert first["_bronze_source"] == "inpe_queimadas"
    bad = spark.table(QUARANTINE).first()
    assert (bad["problem"], bad["source_line_number"]) == ("wrong number of values", 5)

    file = spark.table(FILES).first()
    assert (file["received_rows"], file["received_foci"], file["quarantined_lines"]) == (3, 2, 1)
    assert file["status"] == "COMPLETE_WITH_ISSUES"
    run = spark.table("bronze.fire_runs").first()
    assert (run["run_id"], run["files"], run["fire_rows"]) == ("first", 1, 3)


def test_bronze_wrong_header_goes_to_quarantine(spark, tmp_path):
    # Same as INMET: a file with a new header is QUARANTINED, its lines are not lost.
    write(tmp_path, 2020, [line("a"), line("b")], header="id,lat,lon")
    run_fire_bronze(spark, str(tmp_path), [2020])
    assert spark.table(FOCI).count() == 0
    assert spark.table(QUARANTINE).where("problem = 'unexpected header'").count() == 2
    assert spark.table(FILES).first()["status"] == "QUARANTINED"


def test_bronze_missing_year_changes_nothing(spark, tmp_path):
    write(tmp_path, 2020, [line("a")])
    run_fire_bronze(spark, str(tmp_path), [2020])
    (tmp_path / "2021" / "fire").mkdir(parents=True)                 # a year without a file
    with pytest.raises(Exception):
        run_fire_bronze(spark, str(tmp_path), [2020, 2021])
    assert [r["foco_id"] for r in spark.table(FOCI).collect()] == ["a"]   # old data is still there


def test_bronze_reload_one_year_keeps_the_other(spark, tmp_path):
    write(tmp_path, 2020, [line("a")])
    write(tmp_path, 2021, [line("b", time="2021-08-01 17:00:00")])
    run_fire_bronze(spark, str(tmp_path), [2020, 2021])
    write(tmp_path, 2021, [line("c", time="2021-08-01 17:00:00")])
    run_fire_bronze(spark, str(tmp_path), [2021])
    assert sorted(r["foco_id"] for r in spark.table(FOCI).collect()) == ["a", "c"]


def stations(spark):
    # One station near the fake fires (lat -10.513, lon -53.483), for the nearest station step.
    return spark.createDataFrame([("A001", 2020, -10.5, -53.5, True)],
                                 "station_code string, source_year int, latitude double, "
                                 "longitude double, in_brazil boolean")


def test_silver_types_state_and_local_date(spark, tmp_path):
    write(tmp_path, 2020, [line("a", state="MATO GROSSO", time="2020-06-16 02:00:00")])
    run_fire_bronze(spark, str(tmp_path), [2020])
    row = run_fire_silver(spark, stations=stations(spark)).first()

    assert row["lat"] == -10.513
    assert row["state"] == "MT"                       # full name -> UF, like INMET
    assert row["timezone"] == "America/Cuiaba"
    assert str(row["local_date"]) == "2020-06-15"     # 02:00 UTC is still the 15th in Cuiabá
    assert row["dq_issue"] is None
    assert row["nearest_station_code"] == "A001"


def test_silver_marks_problems_and_removes_doubles(spark, tmp_path):
    write(tmp_path, 2020, [
        line("ok"),
        line("ok"),                                   # the same focus twice
        line("x1", state="ATLANTIS"),
        line("x2", lat="  45.0 "),
        line("x3", biome="Sahara"),
        line("x4", time="not a time"),
    ])
    run_fire_bronze(spark, str(tmp_path), [2020])
    silver = run_fire_silver(spark, stations=stations(spark))
    issues = {r["foco_id"]: r["dq_issue"] for r in silver.collect()}

    assert issues == {"ok": None, "x1": "unknown_state", "x2": "outside_brazil",
                      "x3": "unknown_biome", "x4": "bad_timestamp"}
    save_fire_silver(spark, silver, [2020])
    assert spark.table("silver.fire_foci").count() == 5
