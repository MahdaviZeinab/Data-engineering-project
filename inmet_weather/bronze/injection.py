"""
Step 0: download the INMET yearly zip files and unzip the CSV files.

  data/raw/<year>.zip     the original archive (kept as it is)
  data/raw/<year>/*.CSV   the files the Bronze job reads
  data/raw/<year>/_DONE   written last: the year is complete

Safe to run again: finished years are skipped.
  python bronze/injection.py                # 2020-2025
  python bronze/injection.py --years 2025   # one year
"""
import argparse
import shutil
import time
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
# <project>/data/raw, wherever you start the script from
FOLDER = Path(__file__).resolve().parents[1] / "data" / "raw"
# Written last, after all CSV files of a year are unzipped and counted.
DONE = "_DONE"


def download(year):
    destination = FOLDER / f"{year}.zip"
    temporary = FOLDER / f"{year}.zip.part"
    if destination.exists() and zipfile.is_zipfile(destination):
        print(f"{year}: already downloaded.")
        return True

    for attempt in range(1, 4):
        try:
            print(f"{year}: downloading (attempt {attempt}/3)...", flush=True)
            with requests.get(URL.format(year=year), stream=True, timeout=(30, 120)) as response:
                response.raise_for_status()
                size = response.headers.get("Content-Length", "")
                with temporary.open("wb") as file, tqdm(
                    total=int(size) if size.isdigit() else None,
                    desc=str(year), unit="B", unit_scale=True, unit_divisor=1024,
                ) as progress:
                    for chunk in response.iter_content(1024 * 1024):
                        file.write(chunk)
                        progress.update(len(chunk))

            # Check the zip, and only then give it its final name.
            with zipfile.ZipFile(temporary) as archive:
                if archive.testzip() is not None:
                    raise zipfile.BadZipFile("ZIP integrity check failed.")
            temporary.replace(destination)
            return True
        except (requests.RequestException, zipfile.BadZipFile, OSError) as error:
            print(f"{year}: {error}")
            time.sleep(3)
    print(f"{year}: failed after 3 attempts. Run again to retry.")
    return False


def unzip(year):
    """Put every CSV directly in data/raw/<year>/ (only the file name is used,
    so sub-folders or strange paths inside the zip cannot cause problems).

    Crash-safe: files go to data/raw/<year>.tmp/ first. Only when the number of
    CSV files matches the zip is a _DONE file written and the folder renamed to
    data/raw/<year>/. A half-finished unzip is never mistaken for a finished one.
    """
    target = FOLDER / str(year)
    temporary = FOLDER / f"{year}.tmp"
    shutil.rmtree(temporary, ignore_errors=True)   # leftovers of a crashed run
    temporary.mkdir(parents=True)

    with zipfile.ZipFile(FOLDER / f"{year}.zip") as archive:
        members = [m for m in archive.infolist()
                   if not m.is_dir() and m.filename.upper().endswith(".CSV")]
        sources = {}   # file name -> path inside the zip
        for member in members:
            name = Path(member.filename).name
            data = archive.read(member)
            if name in sources:
                # Same file name in two zip folders. Bronze needs the exact
                # INMET name, so we cannot rename it: keep one copy if they are
                # the same, otherwise stop instead of losing data.
                if (temporary / name).read_bytes() != data:
                    raise RuntimeError(
                        f"{year}: '{member.filename}' and '{sources[name]}' have the "
                        f"same name but different content. Check the zip by hand.")
                print(f"{year}: skipped '{member.filename}' (same as '{sources[name]}').")
                continue
            sources[name] = member.filename
            (temporary / name).write_bytes(data)

    # Compare with the zip: one file per unique CSV name.
    written = count_csv(temporary)
    if written != len(sources):
        raise RuntimeError(f"{year}: zip has {len(sources)} CSV files, "
                           f"but {written} were written.")
    (temporary / DONE).write_text(f"{written}\n")

    shutil.rmtree(target, ignore_errors=True)       # old or half-finished folder
    temporary.rename(target)
    print(f"{year}: {written} CSV files in {target}")


def count_csv(folder):
    return len(list(folder.glob("*.[cC][sS][vV]")))


def is_unzipped(year):
    """Finished = _DONE exists and still matches the number of CSV files."""
    done = FOLDER / str(year) / DONE
    try:
        return int(done.read_text()) == count_csv(done.parent)
    except (OSError, ValueError):
        return False


def land(year):
    if is_unzipped(year):
        print(f"{year}: already unzipped.")
        return True
    if not download(year):
        return False
    try:
        unzip(year)
    except (RuntimeError, OSError, zipfile.BadZipFile) as error:
        print(error)
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, nargs="*", default=list(range(2020, 2026)))
    years = parser.parse_args().years
    FOLDER.mkdir(parents=True, exist_ok=True)
    failed = [y for y in years if not land(y)]
    if failed:
        raise SystemExit(f"Failed years: {failed}")
