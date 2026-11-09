"""
Tests for the unzip step of injection.py, with small fake zip files (no network).
Run:  python -m pytest -q tests/test_injection.py
"""
import zipfile

import pytest
import os

from bronze import injection

A = "INMET_SE_SP_A001_SAO PAULO_01-01-2020_A_31-12-2020.CSV"
B = "INMET_SE_SP_A002_CAMPINAS_01-01-2020_A_31-12-2020.CSV"


@pytest.fixture()
def raw(tmp_path, monkeypatch):
    monkeypatch.setattr(injection, "FOLDER", tmp_path)
    monkeypatch.setattr(injection, "download", lambda year: True)   # never go online
    return tmp_path


def make_zip(raw, year, files):
    with zipfile.ZipFile(raw / f"{year}.zip", "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)


def test_unzip_writes_done_and_no_temp_folder(raw):
    make_zip(raw, 2020, {f"2020/{A}": "a", f"2020/{B}": "b", "2020/readme.txt": "x"})
    assert injection.land(2020)
    folder = raw / "2020"
    assert sorted(p.name for p in folder.iterdir()) == sorted([A, B, "_DONE"])
    assert (folder / "_DONE").read_text().strip() == "2"
    assert not (raw / "2020.tmp").exists()


def test_half_finished_folder_is_unzipped_again(raw):
    make_zip(raw, 2020, {A: "a", B: "b"})
    (raw / "2020").mkdir()
    (raw / "2020" / A).write_text("a")   # crash after the first file, no _DONE
    assert injection.land(2020)
    assert (raw / "2020" / B).read_text() == "b"


def test_missing_file_after_done_is_unzipped_again(raw):
    make_zip(raw, 2020, {A: "a", B: "b"})
    assert injection.land(2020)
    (raw / "2020" / B).unlink()          # _DONE says 2, only 1 left
    assert not injection.is_unzipped(2020)
    assert injection.land(2020)
    assert (raw / "2020" / B).exists()


def test_leftover_temp_folder_is_cleaned(raw):
    make_zip(raw, 2020, {A: "a"})
    (raw / "2020.tmp").mkdir()
    (raw / "2020.tmp" / "junk.CSV").write_text("old")
    assert injection.land(2020)
    assert sorted(p.name for p in (raw / "2020").iterdir()) == [A, "_DONE"]


def test_same_name_same_content_keeps_one_copy(raw):
    make_zip(raw, 2020, {f"x/{A}": "a", f"y/{A}": "a", B: "b"})
    assert injection.land(2020)
    assert (raw / "2020" / "_DONE").read_text().strip() == "2"


def test_same_name_different_content_stops_the_year(raw):
    make_zip(raw, 2020, {f"x/{A}": "a", f"y/{A}": "DIFFERENT"})
    assert not injection.land(2020)
    assert not (raw / "2020").exists()   # nothing for Bronze to read by mistake



def test_zip_without_csv_fails(raw):
    make_zip(raw, 2020, {"readme.txt": "x"})
    assert not injection.land(2020)
    assert not (raw / "2020").exists()


def test_corrupt_zip_fails_and_is_deleted(raw):
    with zipfile.ZipFile(raw / "2020.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(A, os.urandom(3000).hex())
    data = bytearray((raw / "2020.zip").read_bytes())
    for i in range(45, 90):              # break the compressed data in the middle
        data[i] ^= 0xFF
    (raw / "2020.zip").write_bytes(data)

    assert not injection.land(2020)      # no crash
    assert not (raw / "2020.zip").exists()   # next run downloads it again
    assert not (raw / "2020").exists()