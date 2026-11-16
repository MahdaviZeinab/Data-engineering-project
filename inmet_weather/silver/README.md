# Silver layer

Reads the Spark tables `bronze.inmet_hourly` and `bronze.inmet_files` (with `spark.table`) and writes `data/lake/<env>/silver/`.
One row in `silver/weather_hourly` = one station, one hour.

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
| max < min (temp, dew point, pressure, humidity) | max can't be below min in the same hour |
| `dew_point_c` > `temp_c` + 0.5 | dew point can't be above air temperature (0.5 for sensor error) |
| small negative radiation → 0 (counted) | sensor noise at night |

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
| `silver/station_history` | one station, one year | station info as that year's file said it, plus `name_changed`, `location_changed`, `in_brazil`, `timezone` |
| `silver/station` | one station | the latest info, plus `first_year`, `last_year`, `years`, `name_changes`, `location_changes` |

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
