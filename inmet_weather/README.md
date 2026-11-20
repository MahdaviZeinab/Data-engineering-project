# INMET weather – medallion pipeline (DAT535)

Hourly weather data from INMET (Brazil), 2020–2025: about 3,470 station files, 2.2 GB.
Batch processing with Apache Spark and GitHub Actions.

```
INMET website ─► bronze/injection.py ─► data/raw/<year>/*.CSV ─► bronze/bronze_ingest.py ─► Spark tables bronze.*
                                                     silver/ reads spark.table("bronze.inmet_hourly") ─► Spark tables silver.*
                                                     gold/   (later)
```

Each layer saves its result as **Spark tables** (`saveAsTable`, Parquet, split by year).
The next layer reads them by name with `spark.table(...)`, not from a folder path.
Every layer starts Spark and saves its tables with `common/spark.py`, so they all see the same tables.

## Folders

```
inmet_weather/
├── bronze/                 Bronze layer: getting the data in
│   ├── injection.py        download + unzip the yearly INMET files
│   └── bronze_ingest.py    Spark job: raw files -> Bronze Spark tables
├── common/
│   ├── spark.py            start_spark() + save_table(): every layer uses the same Spark tables
│   └── checks.py           small checks we run on a table before we save it
├── silver/                 Silver layer (next step)
├── gold/                   Gold layer (later)
├── tests/                  tests for all layers
├── data/                   NOT in Git (see .gitignore)
│   ├── raw/<year>/         the original INMET CSV files
│   └── lake/<env>/         the output: warehouse/ (Spark tables) + metastore_db/ (table list)
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

| Spark table | One row per | Content |
|---|---|---|
| `bronze.inmet_hourly` | station-hour | the 19 values as text (as INMET wrote them), source file, line number, `_bronze_*` columns |
| `bronze.inmet_files` | source file | station info (lines 1-8), header check, expected / received / missing hours, `status` (COMPLETE, COMPLETE_WITH_ISSUES, INCOMPLETE, QUARANTINED) |
| `bronze.inmet_quarantine` | bad line | the raw line and the problem |
| `bronze.runs` | run | when it ran, which years, how many files and rows |

All tables are Parquet, split by `source_year`. Loading one year again only replaces that year.
Saving happens in two steps: first all new rows go to `<table>_new` tables and are checked,
and only then do they replace the old years. If the job crashes or a check fails in step one,
the old data is not touched.
In step two the three tables change together: the old rows are first copied to `<table>_old`,
and if replacing one table fails, the old rows are put back in all three.
If the job is killed in the middle (e.g. the VM restarts), `check_tables_match` finds years where
the tables hold different runs; then run Bronze again for those years.
Column names are simple versions of the Portuguese names (Spark tables do not allow commas
in column names); the values are not changed.

Look at the tables:

```python
from common.spark import start_spark
spark = start_spark("look", "data/lake/dev")
spark.sql("SHOW TABLES IN bronze").show()
spark.table("bronze.inmet_files").show()
```

Note: the table list is kept in a small Derby database (`metastore_db/`), which allows only
one Spark program at a time. Run Bronze, then Silver, not both at once.
## GitHub Actions: dev and prod

The workflows are in `.github/workflows/` at the repository root. Both run on our OpenStack VM
(self-hosted runner, labels `self-hosted, linux, openstack`). Data is kept on the VM in `/data/inmet`.

| | Dev | Prod |
|---|---|---|
| Workflow | `dev.yml` | `prod.yml` |
| Runs after | a push to any branch **except** `main` | a push to `main` (a merged Pull Request) |
| GitHub Environment | `dev` (no rules) | `main` (**Required reviewers**) |
| Years | 2025 only (fast) | all years 2020-2025 |
| Writes to | `/data/inmet/lake/dev` | `/data/inmet/lake/prod` |

Each run has two jobs: `bronze` (tests, download, Bronze tables), then `silver` (Silver tables).
`silver` starts only if `bronze` worked, so Silver always reads the Bronze tables of the same run.

Branch flow:

1. Work on a feature branch and push. The dev pipeline runs and changes only `lake/dev`.
2. Open a Pull Request into `main` and merge it when dev is green.
3. The prod pipeline starts and **waits**. A reviewer opens the run in the Actions tab,
   clicks **Review deployments**, then **Approve and deploy**.

Raw files (`/data/inmet/raw`) and the Python environment (`/data/inmet/venv`) are shared by dev and prod.
