"""
Step 0: download the INMET yearly zip files and unzip the CSV files.

  data/raw/<year>.zip     the original archive (kept as it is)
  data/raw/<year>/*.CSV   the files the Bronze job reads

Safe to run again: finished years are skipped.
  python bronze/injection.py                # 2020-2025
  python bronze/injection.py --years 2025   # one year
"""
import argparse
import time
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
# <project>/data/raw, wherever you start the script from
FOLDER = Path(__file__).resolve().parents[1] / "data" / "raw"


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
    so sub-folders or strange paths inside the zip cannot cause problems)."""
    target = FOLDER / str(year)
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(FOLDER / f"{year}.zip") as archive:
        for member in archive.infolist():
            name = Path(member.filename).name
            if name.upper().endswith(".CSV"):
                (target / name).write_bytes(archive.read(member))
    print(f"{year}: {len(list(target.glob('*.[cC][sS][vV]')))} CSV files in {target}")


def land(year):
    folder = FOLDER / str(year)
    if folder.is_dir() and any(folder.glob("*.[cC][sS][vV]")):
        print(f"{year}: already unzipped.")
        return True
    if not download(year):
        return False
    unzip(year)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, nargs="*", default=list(range(2020, 2026)))
    years = parser.parse_args().years
    FOLDER.mkdir(parents=True, exist_ok=True)
    failed = [y for y in years if not land(y)]
    if failed:
        raise SystemExit(f"Failed years: {failed}")
