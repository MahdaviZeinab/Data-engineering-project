# Silver layer

Reads the Spark tables `bronze.inmet_hourly` and `bronze.inmet_files` (with `spark.table`) and saves
the Spark tables `silver.inmet_hourly`, `silver.inmet_quarantine`, `silver.quality_report`,
`silver.station_history` and `silver.station` (with `saveAsTable`).
The full story, with numbers, is in [`SILVER_REPORT.md`](SILVER_REPORT.md).
One row in `silver.inmet_hourly` = one station, one hour.

All rules live in [`contract.py`](contract.py). The job reads them from there, so this README and the code agree.

## Schema check

At the start, Silver compares the Bronze columns with `BRONZE_COLUMNS`.
A missing or new column stops the job with a clear error.
**Why:** a silent schema change would give wrong or empty columns in Gold.

## Key and missing values

| Rule | Value | Why |
|---|---|---|
| `KEY` | `station_code` + `observed_at_utc` | one row per station per hour |
| `MISSING_CODES` | `""`, `-9999` | INMET's "no value"; becomes null, not a number |
| Lineage | `source_file`, `source_line_number`, `_bronze_run_id`, `_silver_run_id` | every row points back to its Bronze line |

## Names and types (`RENAME`)

| Bronze (Portuguese) | Silver (English) | Type | Unit |
|---|---|---|---|
| `data` + `hora_utc` | `observed_at_utc` | timestamp | UTC |
| `precipitacao_total_mm` | `precip_mm` | double | mm |
| `pressao_estacao_mb` | `pressure_mb` | double | mB |
| `pressao_max_mb` | `pressure_max_mb` | double | mB |
| `pressao_min_mb` | `pressure_min_mb` | double | mB |
| `radiacao_global_kj_m2` | `radiation_kj_m2` | double | kJ/m² |
| `temperatura_ar_c` | `temp_c` | double | °C |
| `temperatura_orvalho_c` | `dew_point_c` | double | °C |
| `temperatura_max_c` | `temp_max_c` | double | °C |
| `temperatura_min_c` | `temp_min_c` | double | °C |
| `temperatura_orvalho_max_c` | `dew_point_max_c` | double | °C |
| `temperatura_orvalho_min_c` | `dew_point_min_c` | double | °C |
| `umidade_max_pct` | `humidity_max_pct` | double | % |
| `umidade_min_pct` | `humidity_min_pct` | double | % |
| `umidade_relativa_pct` | `humidity_pct` | double | % |
| `vento_direcao_graus` | `wind_dir_deg` | double | degrees |
| `vento_rajada_ms` | `wind_gust_ms` | double | m/s |
| `vento_velocidade_ms` | `wind_speed_ms` | double | m/s |

**Why English names:** short, no spaces or accents, easy to use in Gold and SQL.

## Invalid values (`INVALID`): impossible, so the value becomes null

| Rule | Why |
|---|---|
| `precip_mm`, `wind_speed_ms`, `wind_gust_ms` < 0 | can't be negative |
| humidity (all 3) < 0 or > 100 | it is a percentage |
| `wind_dir_deg` < 0 or > 360 | compass direction |
| `pressure_*` < 1 mB | pressure can't be 0 |
| `radiation_kj_m2` > 5,000 | the sun gives about 4,900 kJ/m² per hour at most |
| max < min (temp, dew point, pressure, humidity) | max can't be below min in the same hour (we don't know which is wrong, so both become null) |
| `dew_point_c` > `temp_c` + 0.5 | dew point can't be above air temperature (0.5 for sensor error) |
| small negative radiation → 0 (flag `radiation_negative_set_to_0`) | sensor noise at night |

Only that one value becomes null and gets `invalid_<column>` in `dq_flags`. The rest of the row stays.

## Unusual values (`UNUSUAL`): possible but rare, so kept with a flag

| Rule | Why |
|---|---|
| `precip_mm` > 100 | very heavy rain (WMO record is 305 mm in one hour) |
| temperature < −10 or > 45 °C | rare in Brazil |
| `wind_gust_ms` > 40 | storm strength |
| `pressure_*` outside 700–1,100 mB | far from normal station pressure |
| temperature change > 10 °C from the hour before | sudden jump, check it |

These are start values. Update them with the S1 percentiles.

**Code:** [`value_rules.py`](value_rules.py). Every row gets a list column `dq_flags`,
for example `["invalid_humidity_pct", "unusual_precip_mm"]`. An empty value (missing) never gets a flag.
`count_flags()` gives the number of flags per year; the Silver job prints it in the log.

## Quarantine (Silver 9, [`quarantine.py`](quarantine.py)): rows we can not use

Each row gets one reason in `dq_issue` (the first one that is true). Good rows go to
`silver.inmet_hourly`, the others to `silver.inmet_quarantine`. Nothing is thrown away.

| Reason | Why we can't use the row |
|---|---|
| `bad_timestamp` | wrong date or hour, so we don't know when it was measured |
| `unknown_station` | station not in `bronze.inmet_files` (no name, state or place) |
| `unknown_timezone` | no time zone for the station, so no local day |
| `not_in_brazil` | outside Brazil (e.g. C891 in Antarctica): good data, but not for our question |
| `all_values_empty` | all 17 measurements are empty (after Silver 8) |

## Time zones (`STATE_TIMEZONE`, `STATION_TIMEZONE_OVERRIDE`)

| Zone | States |
|---|---|
| `America/Sao_Paulo` (UTC−3) | all other states and DF |
| `America/Cuiaba`, `America/Campo_Grande`, `America/Porto_Velho`, `America/Boa_Vista`, `America/Manaus` (UTC−4) | MT, MS, RO, RR, AM |
| `America/Rio_Branco` (UTC−5) | AC |

**Why:** Brazil has four time zones. With UTC−3 for all, rain in Acre or Mato Grosso can land on the wrong day.
Stations in western Amazonas (UTC−5) or Fernando de Noronha (UTC−2) go in `STATION_TIMEZONE_OVERRIDE`.

## Stations (Silver 7, [`stations.py`](stations.py))

Every INMET file starts with 8 lines about the station (name, state, latitude, ...).
Bronze keeps them in the table `bronze.inmet_files` (one row per file = one station, one year).

| Table | One row = | What it holds |
|---|---|---|
| `silver.station_history` | one station, one year | station info as that year's file said it, plus `name_changed`, `location_changed`, `in_brazil`, `timezone` |
| `silver.station` | one station | the latest info, plus `first_year`, `last_year`, `years`, `name_changes`, `location_changes` |

| Rule | Value | Why |
|---|---|---|
| numbers | `,` → `.`, `-9999`/`""` → null | same as the hourly values |
| two files for one station in one year | keep the one with the latest `end_date` | one row per station per year |
| `name_changed` | name differs from the year before (case and spaces ignored) | track renames |
| `location_changed` | latitude/longitude moved > `LOCATION_TOLERANCE_DEG` (0.001° ≈ 100 m) or altitude > `ALTITUDE_TOLERANCE_M` (1 m) | small differences are only rounding |
| `timezone` | `STATION_TIMEZONE_OVERRIDE`, else `STATE_TIMEZONE` | same rule as Silver 5 |
| `in_brazil` | inside `BRAZIL_BOX` (incl. islands); no coordinates → false | finds wrong coordinates, e.g. a lost minus sign |

The station tables are small, so they are always built from **all** years, also when the job runs with `--years`.
They run at the end of `silver_build`, or alone with `python -m silver.stations`.

Saving works like Bronze: all Silver tables are first written to `<table>_new`, then replaced together.
If replacing one table fails, all of them get their old rows back.
Before Silver reads Bronze, `check_tables_match` stops the job if the Bronze tables hold different runs
(a killed Bronze run); then run Bronze again for those years.

## Fires (`fire_build.py`, rules in [`fire_contract.py`](fire_contract.py))

Reads `bronze.fire_foci` and saves `silver.fires` and `silver.fire_quarantine` (together), split by `source_year`.
One row = one fire point (focus) seen by the INPE reference satellite.

| Rule | Value | Why |
|---|---|---|
| `KEY` | `foco_id` | INPE's id, unique per fire focus |
| text | `trim`, empty → null | INPE writes spaces around numbers (`"   -3.650000 "`) |
| types | `id_bdq` long, `lat`/`lon` double, `data_pas` → `observed_at_utc` (UTC) | all hours are 15–19 UTC, the afternoon satellite pass |
| `state` | `STATE_TO_UF`: `MATO GROSSO` → `MT` | INMET uses the UF code; without this, a join on state finds **no rows and no error** |
| `timezone`, `local_date` | from `STATE_TIMEZONE` (same as weather) | fires and weather land on the same local day |
| `pais` | dropped | always `Brasil` |
| duplicates | one row per `foco_id`: the one loaded last | a year loaded twice must not count fires twice |

### Quarantine and the state check ([`state_check.py`](state_check.py))

A row with a problem gets it in `dq_issue` (first problem only) and moves to `silver.fire_quarantine`.
Nothing is thrown away: every Bronze row is in `silver.fires`, in the quarantine, or a removed duplicate.

| `dq_issue` | Meaning |
|---|---|
| `missing_id` | no `foco_id` |
| `bad_timestamp` | the date or time can't be read |
| `bad_coordinates` | no `lat` or `lon` |
| `outside_brazil` | outside `BRAZIL_BOX` |
| `unknown_state` | the state name is not in `STATE_TO_UF` |
| `wrong_state` | the point is not inside the box of its state (`STATE_BOX` + `STATE_BOX_MARGIN_DEG` = 0.1°, ~11 km) |
| `unknown_biome` | not one of the 6 `BIOMES` |

**Why a box per state, not the real border:** the real borders need a map file and a geo library.
A box is 4 numbers per state and plain Spark. It finds the big mistakes (wrong state name, lat and lon swapped),
but not a fire a few km on the wrong side of a border, because the boxes of neighbouring states overlap.

### The same fire seen twice ([`fire_events.py`](fire_events.py))

A big fire is often seen as several points next to each other (one per satellite pixel).
Points on the same `local_date` in the same `EVENT_GRID_DEG` square (0.01°, about 1 km) are **one fire event**.
Every point keeps its row, and gets:

| Column | Meaning |
|---|---|
| `fire_event_id` | `foco_id` of the first point of the event |
| `points_in_event` | how many points the event has |
| `first_in_event` | true for one point per event: count fires with `where first_in_event` |

**Why squares:** a window over (day, square) is simple and fast. Two points 100 m apart but on
two sides of a square's edge become two events; with 1 km squares this is rare.

### Nearest weather station ([`nearest_station.py`](nearest_station.py))

Every fire gets `nearest_station_code` and `distance_km` (still one row per fire), so Gold can join
it with the weather of that station. It reads `silver.station_history`, so run `silver.stations` first.

| Rule | Value | Why |
|---|---|---|
| stations used | `in_brazil` and the same `source_year` as the fire | no wrong coordinates, no station that did not exist that year |
| distance | haversine, in km | real distance on the earth, not in degrees |
| `GRID_DEG` | 1.0 | squares of about 111 km, see below |
| `MAX_DISTANCE_KM` | 90 | further away, the station's weather says little about the fire |
| same distance | lowest `station_code` | every run gives the same answer |

**Why a grid:** comparing every fire with every station is about 136,000 × 600 pairs a year.
Instead, each station is put in its own 1° square and the 8 squares around it, and a fire is only
compared with the stations of its own square (a few at most). Below 90 km, a station is always in
one of those 9 squares (1° of longitude is still ~92 km in the far south), so no close station is missed.
The station table is small, so it is broadcast to every worker.

A fire with no station within 90 km gets null and `dq_issue = no_station_nearby`.
It stays in `silver.fires`: the fire is fine, there is just no weather station close to it.

Real 2020 file: 222,797 rows, 0 in quarantine, 0 duplicates, 0 rows with a `dq_issue`.
