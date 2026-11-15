# Silver layer

Reads `data/lake/<env>/bronze/` and writes `data/lake/<env>/silver/`.
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
| `Data` + `Hora UTC` | `observed_at_utc` | timestamp | UTC |
| `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)` | `precip_mm` | double | mm |
| `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)` | `pressure_mb` | double | mB |
| `PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)` | `pressure_max_mb` | double | mB |
| `PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)` | `pressure_min_mb` | double | mB |
| `RADIACAO GLOBAL (Kj/m²)` | `radiation_kj_m2` | double | kJ/m² |
| `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)` | `temp_c` | double | °C |
| `TEMPERATURA DO PONTO DE ORVALHO (°C)` | `dew_point_c` | double | °C |
| `TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)` | `temp_max_c` | double | °C |
| `TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)` | `temp_min_c` | double | °C |
| `TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)` | `dew_point_max_c` | double | °C |
| `TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)` | `dew_point_min_c` | double | °C |
| `UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)` | `humidity_max_pct` | double | % |
| `UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)` | `humidity_min_pct` | double | % |
| `UMIDADE RELATIVA DO AR, HORARIA (%)` | `humidity_pct` | double | % |
| `VENTO, DIREÇÃO HORARIA (gr) (° (gr))` | `wind_dir_deg` | double | degrees |
| `VENTO, RAJADA MAXIMA (m/s)` | `wind_gust_ms` | double | m/s |
| `VENTO, VELOCIDADE HORARIA (m/s)` | `wind_speed_ms` | double | m/s |

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
