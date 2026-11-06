# INMET weather – medallion pipeline (DAT535)

Hourly weather data from INMET (Brazil), 2020–2025: about 3,470 station files, 2.2 GB.
Batch processing with Apache Spark and GitHub Actions.

```
INMET website ─► bronze/injection.py ─► data/raw/<year>/*.CSV ─► bronze/bronze_ingest.py ─► data/lake/<env>/bronze/
                                                                   silver/ ─► data/lake/<env>/silver/
                                                                   gold/   ─► data/lake/<env>/gold/
```

## Folders

```
inmet_weather/
├── bronze/                 Bronze layer: getting the data in
│   ├── injection.py        download + unzip the yearly INMET files
│   ├── bronze_ingest.py    Spark job: raw files -> Bronze tables
│   └── BRONZE_REPORT.md    what the Bronze code does, how and why
├── silver/                 Silver layer (next step)
├── gold/                   Gold layer (later)
├── tests/                  tests for all layers
├── data/                   NOT in Git (see .gitignore)
│   ├── raw/<year>/         the original INMET CSV files
│   └── lake/<env>/         the output: bronze/, silver/, gold/
├── requirements.txt
└── pytest.ini
```

Code is in `bronze/`, `silver/` and `gold/`. Data is only in `data/`, so the code folders stay small,
and Git never tries to save 2.2 GB of CSV files.

## Set up once

```bash
cd inmet_weather
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt      # needs Java 8, 11 or 17
```

## Run (from the inmet_weather folder)

```bash
python bronze/injection.py                    # download + unzip (skips finished years)
python -m bronze.bronze_ingest                # Bronze, all years, dev
python -m bronze.bronze_ingest --years 2025   # only 2025 (other years are not touched)
python -m bronze.bronze_ingest --env prod     # production folder
python -m pytest -q                           # tests
```

If `DAT535_OUTPUT_DIR` is set (as in the course lab CI), the output goes there.

## Bronze output

| Table | One row per | Content |
|---|---|---|
| `inmet_hourly/` | station-hour | the 19 values as text with the original Portuguese column names, source file, line number, `_bronze_*` columns |
| `inmet_files/` | source file | station info, checksum, expected / received / missing / extra / duplicate hours, status |
| `inmet_quarantine/` | rejected line | the raw line and the problem |
| `_runs/<run>.json` | run | summary of the run |

All tables are Parquet, split into one folder per year (`source_year=2020`, …).

## GitHub Actions

The workflow is in `.github/workflows/` at the repository root:

- `bronze-ingest.yml` runs the Bronze pipeline on our OpenStack VM (self-hosted runner, labels
  `self-hosted, linux, openstack`). Data is kept on the VM in `/data/inmet`.

How to set up the VM and the runner: see [RUNNER_SETUP.md](RUNNER_SETUP.md).
