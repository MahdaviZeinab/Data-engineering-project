"""
Step 0: download the yearly zip files and unzip the CSV files.
Works for both sources: INMET weather (default) and INPE fires (--source fire).

The zip files stay in data/raw. The CSV files go to one folder per year and source:

  data/raw/<year>.zip             the original INMET zip file (kept as it is)
  data/raw/fire_<year>.zip        the original INPE fire zip file
  data/raw/<year>/inmet/*.CSV     the CSV files the Bronze job reads
  data/raw/<year>/inmet/_DONE     written last: it means the year is complete
  data/raw/<year>/fire/*.csv      the same for the fire files
  data/raw/<year>/fire/_DONE

You can run it again at any time: finished years are skipped.
  python bronze/injection.py                              # INMET, 2020-2025
  python bronze/injection.py --years 2025                 # INMET, one year
  python bronze/injection.py --source fire --years 2025   # INPE fires, one year
"""
import argparse
import shutil
import time
import zipfile
import zlib
from pathlib import Path

import requests
from tqdm import tqdm

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"      # <project>/data/raw

# Our two sources: name -> download address. Both give one zip per year,
# so the same steps work for both. The name is also the folder name.
SOURCES = {
    "inmet": "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip",
    "fire": ("https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/"
             "Brasil_sat_ref/focos_br_ref_{year}.zip"),
}
SOURCE = "inmet"    # main() changes it when --source fire is given
DONE = "_DONE"      # file we write last, when a year is complete
TRIES = 3           # how many times we try to download a year


# Where the files of one year go
# ---------------------------------------------------------------------------
def year_folder(year):
    # data/raw/<year>
    return RAW / str(year)


def zip_file(year):
    # data/raw/<year>.zip for INMET, data/raw/fire_<year>.zip for the fire files
    prefix = "" if SOURCE == "inmet" else f"{SOURCE}_"
    return RAW / f"{prefix}{year}.zip"


def csv_folder(year):
    # data/raw/<year>/inmet/ (or fire/): the folder Bronze reads
    return year_folder(year) / SOURCE


# Step 1: download the zip file
# ---------------------------------------------------------------------------
def download(year):
    # Download the zip file of one year. We try up to 3 times, because the
    # INMET website is sometimes slow. Returns True when the zip is there.
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
    # Download the zip to "<year>.zip.part" first, with a progress bar.
    # Only when the zip is complete and not broken do we rename it to "<year>.zip".
    # So a half-downloaded file is never used by mistake.
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
    # Stop with an error if the zip file is broken.
    with zipfile.ZipFile(path) as archive:
        if archive.testzip() is not None:
            raise zipfile.BadZipFile("ZIP integrity check failed.")


# Step 2: unzip the CSV files
# ---------------------------------------------------------------------------
def unzip(year):
    # Unzip all CSV files of one year into data/raw/<year>/inmet/ (or fire/).
    # We unzip into data/raw/<year>/inmet.tmp/ first. Only when every file is there
    # do we write _DONE and rename the folder to data/raw/<year>/inmet/.
    # So Bronze never reads a half-unzipped year.
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
    # Copy every CSV file from the zip straight into the folder.
    # We only use the file name, so folders inside the zip do not matter.
    # Returns how many different CSV files the zip has.
    seen = {}   # file name -> where it was inside the zip
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
    # The zip has the same file name twice (in two folders). Bronze needs the
    # exact INMET name, so we can not rename it. If both copies are the same,
    # we keep one. If they are different, we stop, so we do not lose data.
    if saved_file.read_bytes() != data:
        raise RuntimeError(f"{year}: '{path_in_zip}' and '{first_path}' have the same name "
                           "but different content. Check the zip by hand.")
    print(f"{year}: skipped '{path_in_zip}' (same as '{first_path}').")


def count_csv(folder):
    # How many CSV files are in the folder.
    return len(list(folder.glob("*.[cC][sS][vV]")))


def is_unzipped(year):
    # A year is finished when _DONE exists and still matches the number of CSV files.
    # (If someone deleted a CSV file later, we unzip the year again.)
    done = csv_folder(year) / DONE
    try:
        return int(done.read_text()) == count_csv(done.parent)
    except (OSError, ValueError):
        return False


# The whole step for one year
# ---------------------------------------------------------------------------
def land(year):
    # Get one year ready for Bronze: download it and unzip it.
    # Returns True if the year is ready, False if something went wrong.
    if is_unzipped(year):
        print(f"{year}: already unzipped.")
        return True
    if not download(year):
        return False
    try:
        unzip(year)
    except (zipfile.BadZipFile, zlib.error) as error:
        # The zip itself is broken: delete it, so the next run downloads it again.
        print(f"{year}: broken zip ({error}). Deleted it; run again to download it.")
        zip_file(year).unlink(missing_ok=True)
        return False
    except (RuntimeError, OSError) as error:
        # The zip is fine, but its content is strange, or the disk had a problem.
        print(error)
        return False
    return True


def main():
    # Read the source and the years from the command line and get every year ready.
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
