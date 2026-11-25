# Bronze

Bronze downloads the raw files and loads them into Spark tables.

Nothing is cleaned here. Every value is kept as text, exactly as in the file.


## Files

| File | What it does |
|---|---|
| `injection.py` | downloads and unzips the weather and fire files |
| `bronze_ingest.py` | loads the weather files into Spark tables |
| `fire_ingest.py` | loads the fire files into Spark tables |


## Tables

| Table | One row per |
|---|---|
| `bronze.inmet_hourly` | station and hour |
| `bronze.inmet_files` | weather file |
| `bronze.inmet_quarantine` | broken line |
| `bronze.runs` | run |
| `bronze.fire_foci` | fire |
| `bronze.fire_files` | fire file |
| `bronze.fire_quarantine` | broken line |
| `bronze.fire_runs` | run |


## Good to know

Broken lines are not deleted. They go to a quarantine table.

Each year is saved on its own. Running one year again only replaces that year.

If a run fails, the old data stays.


## Run (from `inmet_weather`)

```bash
python bronze/injection.py
python bronze/injection.py --source fire
python -m bronze.bronze_ingest
python -m bronze.fire_ingest
```
