# Silver

Silver reads the Bronze tables and makes clean data.

Text becomes numbers. Bad rows are put aside. Every rule is in `inmet_contract.py` and `fire_contract.py`.


## Files

| File | What it does |
|---|---|
| `inmet_build.py` | cleans the weather data |
| `inmet_contract.py` | all weather rules |
| `inmet_value_rules.py` | finds impossible and rare values |
| `inmet_quarantine.py` | puts bad weather rows aside |
| `inmet_stations.py` | makes the station tables |
| `inmet_bronze_profile.py` | looks at Bronze before cleaning |
| `fire_build.py` | cleans the fire data |
| `fire_contract.py` | all fire rules |
| `fire_state_check.py` | checks a fire is in its state |
| `fire_station_matching.py` | finds the nearest weather station |
| `fire_events.py` | groups points of the same fire |
| `quality.py` | checks no row is lost |


## Tables

| Table | One row per |
|---|---|
| `silver.inmet_hourly` | station and hour |
| `silver.inmet_quarantine` | bad weather row |
| `silver.quality_report` | year |
| `silver.station_history` | station and year |
| `silver.station` | station |
| `silver.fires` | fire |
| `silver.fire_quarantine` | bad fire row |


## Weather cleaning

- Text becomes numbers: `25,5` becomes 25.5.
- `-9999` and empty text become empty.
- Columns get short English names, for example `temp_c` and `precip_mm`.
- Every station gets its local time. Brazil has 4 time zones.
- Each station and hour is kept only once.


## Impossible and rare values

Impossible values are set to empty. Example: humidity above 100 %.

Rare values are kept, with a flag. Example: more than 100 mm of rain in one hour.

The flags are in the column `dq_flags`.


## Quarantine

Rows we cannot use go to a quarantine table, with a reason.

Weather reasons: bad time, unknown station, no time zone, outside Brazil, all values empty.

Fire reasons: no id, bad time, bad coordinates, outside Brazil, unknown state, wrong state, unknown biome.

Nothing is deleted.


## Fires

- State names become state codes: `MATO GROSSO` becomes `MT`.
- Every fire gets the nearest weather station within 90 km.
- Fire points on the same day within about 1 km are one fire event.


## Safety

Before saving, Silver checks that no row is lost:

```
Bronze rows = good rows + quarantined rows + removed duplicates
```

All tables are saved together. If one fails, the old tables stay.


## Run (from `inmet_weather`)

```bash
python -m silver.inmet_build
python -m silver.fire_build
```
