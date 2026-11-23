import argparse
import shutil
import time
import zipfile
import zlib
from pathlib import Path

import requests
from tqdm import tqdm

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"      # <project>/data/raw

# source name (also the folder name) -> download url
SOURCES = {
    "inmet": "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip",
    "fire": ("https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/"
             "Brasil_sat_ref/focos_br_ref_{year}.zip"),
}
SOURCE = "inmet"    # set by --source
DONE = "_DONE"      # written when a year is complete
TRIES = 3           # download attempts


# Paths
def year_folder(year):
    # data/raw/<year>
    return RAW / str(year)


def zip_file(year):
    # data/raw/<year>.zip for INMET, data/raw/fire_<year>.zip for the fire files
    prefix = "" if SOURCE == "inmet" else f"{SOURCE}_"
    return RAW / f"{prefix}{year}.zip"


def csv_folder(year):
    # data/raw/<year>/inmet/ or fire/
    return year_folder(year) / SOURCE


# Step 1: download
def download(year):
    # retry a few times, the INMET site is sometimes slow
    destination = zip_file(year)
    if destination.exists() and zipfile.is_zipfile(destination):
        print(f"{year}: already downloaded.")
        return True

    RAW.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, TRIES + 1):
        try:
            print(f"{year}: downloading (attempt {attempt}/{TRIES})...", flush=True)
            download_once(year, destination)
            return True
        except (requests.RequestException, zipfile.BadZipFile, OSError) as error:
            print(f"{year}: {error}")
            time.sleep(3)
    print(f"{year}: failed after {TRIES} attempts. Run again to retry.")
    return False


def download_once(year, destination):
    # download to .part first and only rename once the zip is ok
    temporary = zip_file(year).with_suffix(".zip.part")
    with requests.get(SOURCES[SOURCE].format(year=year), stream=True, timeout=(30, 120)) as response:
        response.raise_for_status()
        size = response.headers.get("Content-Length", "")
        total = int(size) if size.isdigit() else None
        with temporary.open("wb") as file, tqdm(total=total, desc=str(year), unit="B",
                                                unit_scale=True, unit_divisor=1024) as bar:
            for chunk in response.iter_content(1024 * 1024):
                file.write(chunk)
                bar.update(len(chunk))

    check_zip(temporary)
    temporary.replace(destination)


def check_zip(path):
    with zipfile.ZipFile(path) as archive:
        if archive.testzip() is not None:
            raise zipfile.BadZipFile("ZIP integrity check failed.")


# Step 2: unzip
def unzip(year):
    # unzip into a .tmp folder, then rename it so Bronze never sees a half-done year
    target = csv_folder(year)
    temporary = year_folder(year) / f"{SOURCE}.tmp"
    shutil.rmtree(temporary, ignore_errors=True)    # leftovers of a crashed run
    temporary.mkdir(parents=True)

    expected = extract_csv_files(year, temporary)
    if expected == 0:
        raise RuntimeError(f"{year}: the zip has no CSV files. Check the zip by hand.")

    written = count_csv(temporary)
    if written != expected:
        raise RuntimeError(f"{year}: zip has {expected} CSV files, but {written} were written.")
    (temporary / DONE).write_text(f"{written}\n")

    shutil.rmtree(target, ignore_errors=True)       # old or half-finished folder
    temporary.rename(target)
    print(f"{year}: {written} CSV files in {target}")


def extract_csv_files(year, folder):
    # flatten folders inside the zip, returns number of distinct CSVs
    seen = {}   # file name -> path in zip
    with zipfile.ZipFile(zip_file(year)) as archive:
        for member in archive.infolist():
            if member.is_dir() or not member.filename.upper().endswith(".CSV"):
                continue
            name = Path(member.filename).name
            data = archive.read(member)
            if name in seen:
                check_same_file(year, folder / name, data, member.filename, seen[name])
                continue
            seen[name] = member.filename
            (folder / name).write_bytes(data)
    return len(seen)


def check_same_file(year, saved_file, data, path_in_zip, first_path):
    # same name twice in the zip: ok if identical, otherwise stop
    if saved_file.read_bytes() != data:
        raise RuntimeError(f"{year}: '{path_in_zip}' and '{first_path}' have the same name "
                           "but different content. Check the zip by hand.")
    print(f"{year}: skipped '{path_in_zip}' (same as '{first_path}').")


def count_csv(folder):
    return len(list(folder.glob("*.[cC][sS][vV]")))


def is_unzipped(year):
    # done if _DONE matches the current number of CSV files
    done = csv_folder(year) / DONE
    try:
        return int(done.read_text()) == count_csv(done.parent)
    except (OSError, ValueError):
        return False


# One year
def land(year):
    if is_unzipped(year):
        print(f"{year}: already unzipped.")
        return True
    if not download(year):
        return False
    try:
        unzip(year)
    except (zipfile.BadZipFile, zlib.error) as error:
        # broken zip, delete it so the next run downloads again
        print(f"{year}: broken zip ({error}). Deleted it; run again to download it.")
        zip_file(year).unlink(missing_ok=True)
        return False
    except (RuntimeError, OSError) as error:
        # bad zip content or disk problem
        print(error)
        return False
    return True


def main():
    global SOURCE
    parser = argparse.ArgumentParser(description="Download and unzip the INMET or INPE files.")
    parser.add_argument("--source", default="inmet", choices=list(SOURCES))
    parser.add_argument("--years", type=int, nargs="*", default=list(range(2020, 2026)))
    args = parser.parse_args()
    SOURCE = args.source
    years = args.years
    failed = [year for year in years if not land(year)]
    if failed:
        raise SystemExit(f"Failed years: {failed}")


if __name__ == "__main__":
    main()
