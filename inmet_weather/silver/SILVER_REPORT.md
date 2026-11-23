# Silver layer: what we did, how, and why

> **Fill in before handing in:** every `TODO (VM)` is a number from a real run on the VM.
> Run `python -m silver.silver_build --env prod` (all years), then copy the numbers from
> `<lake>/_runs/silver_<run_id>.json` and the table `silver.quality_report`.

## 1. Goal

Turn the raw INMET text from Bronze into clean, typed weather data that Gold can trust:
**one row per station per hour**, with numbers instead of text, the right local day for every station,
and a reason for every row we could not use. The question Gold answers:
*"the daily temperature, rain, humidity and wind for each station and state in Brazil, 2020–2025,
in each station's own local day."*

## 2. Contract

All rules are in [`contract.py`](contract.py) (names, missing codes, limits, time zones), and the
[`README.md`](README.md) explains each one. The job reads its rules from the contract, so the code and the
documentation can't disagree. At the start, Silver compares the Bronze columns with `BRONZE_COLUMNS` and
stops if one is missing or new.

## 3. The steps, and why

| # | Step | Code | Why |
|---|---|---|---|
| 1 | Read Bronze | `read_bronze` | all years by default, so a duplicate across years is found |
| 2 | Schema check | `check_schema` | never clean data we don't understand |
| 3 | Text → numbers | `cast_values` | INMET writes `25,5`, `""` and `-9999`; Gold needs numbers |
| 4 | UTC time + local time | `make_time` | Brazil has 4 time zones; rain must land on the right local day |
| 5 | Remove double rows | `remove_duplicates` | one station + one hour = one row, or Gold counts rain twice |
| 6 | Invalid / unusual values | [`value_rules.py`](value_rules.py) | remove impossible values, keep rare ones with a flag |
| 7 | Quarantine | [`quarantine.py`](quarantine.py) | rows we can't use are put aside with a reason, not deleted |
| 8 | Station details | `add_station` | state, region and name, from the file of the same year |
| 9 | Reconcile + checks | [`quality.py`](quality.py) | prove that no row is lost before anything is saved |
| 10 | Station tables | [`stations.py`](stations.py) | current and historical station info |
| 11 | Save together | `save_silver` | all Silver tables change together, or none |
| 12 | Run summary | `write_run_summary` | the numbers of every run, in a small JSON file |

Output tables: `silver.inmet_hourly`, `silver.inmet_quarantine`, `silver.quality_report`,
`silver.station_history`, `silver.station`.

## 4. Before: the Bronze profile (S1)

`silver/profile_bronze.py` saves the `profile.*` tables (empty values, `-9999`, min/max, 1% and 99% per
measurement and year; time formats; hours per station per day).

- Bronze rows per year: `TODO (VM)` (table `profile.bronze_rows_per_year`)
- `-9999` and empty values: `TODO (VM)`
- What we saw (3–5 sentences): `TODO`

## 5. After: the quality report (S11)

Table `silver.quality_report`, one row per year:

| source_year | rows_read_from_bronze | duplicate_rows_removed | bad_rows_moved_to_quarantine | good_rows_saved_in_silver | empty_values_in_bronze | empty_values_in_silver | impossible_values_set_to_empty | rare_values_kept_with_flag |
|---|---|---|---|---|---|---|---|---|
| 2020 | TODO (VM) | | | | | | | |
| … | | | | | | | | |
| 2025 | | | | | | | | |

- `empty_values_in_bronze`: INMET's "no value" codes in Bronze (`""` or `-9999`). In Silver they are empty (null), never `-9999`.
- `empty_values_in_silver`: empty values left in Silver (the missing ones, plus invalid values we made empty).
- Biggest problem we found (1–2 sentences): `TODO`

## 6. Invalid vs unusual values (S8)

- **Invalid** = physically impossible (humidity above 100 %, negative rain, max below min, dew point above
  air temperature …) → that one value becomes empty, the row stays, flag `invalid_<column>` in `dq_flags`.
- **Unusual** = possible but rare (rain above 100 mm/h, temperature below −10 or above 45 °C, a jump of
  more than 10 °C in one hour …) → the value stays, flag `unusual_<column>`.
- Small negative radiation at night is sensor noise → set to 0, flag `radiation_negative_set_to_0`.

We separate the two because a rule like "rain above 150 mm/h is wrong" is too strong: the world record
is 305 mm in one hour. Removing real extreme weather would make Gold lie.

**Example check:** the 20 most extreme unusual values, compared with the hours before/after and a nearby
station. `TODO (VM)`: which look real, which look like errors.

## 7. Time zones (S5)

Every station gets an IANA time zone from its state (`STATE_TIMEZONE`), or from `STATION_TIMEZONE_OVERRIDE`.
`local_time`, `local_date` and `local_hour` come from `observed_at_utc` + that zone.
Test: at `2021-01-01 04:00 UTC`, a station in Acre is still on `2020-12-31`, São Paulo already on `2021-01-01`.

## 8. Reconcile and rerun (S10)

Before saving, the job checks **for every year**:

```
Bronze rows = Silver rows + quarantined rows + removed duplicates
```

If one year does not add up, the job stops and nothing is saved (`reconcile` in `quality.py`).
It also checks one row per station and hour, and that no good row has an empty state or time zone.

**Rerun:** `checksum` adds up a hash of every row (without run ids and load times). Two runs on the same
Bronze data must give the same number. A test runs Silver twice and compares the checksums.

- Reconcile numbers per year: `TODO (VM)` (in the run summary JSON, key `reconcile`)
- Checksum run 1 / run 2: `TODO (VM)` / `TODO (VM)`

## 9. How we save (S0)

Same method as Bronze: every table is first written to `<name>_new`. Then all tables are replaced together.
If one replace fails, all tables get their old rows back (`move_all_from_staging` in `common/spark.py`).
`silver.inmet_hourly`, `silver.inmet_quarantine` and `silver.quality_report` replace only the years of the
run; the station tables are small and always rebuilt from all years.
If the columns of a table changed (for example the new `dq_flags`), the whole table is replaced.

## 10. Performance (S13)

`TODO`: one measured change (time before, time after, why). A good candidate: cache the cleaned rows,
because reconcile, the checks and the quality report each read them again.

## 11. Tests

`python -m pytest -q` (from `inmet_weather/`). Silver tests:
`test_silver_contract.py`, `test_silver_build.py` (S3–S6, whole run, rerun checksum),
`test_value_rules.py` (S8), `test_quarantine.py` (S9), `test_quality.py` (S10), `test_stations.py`
(S7 and saving together). They run in the dev workflow on every push.

## 12. What is still weak

- 6 years of data is short for "unusual" limits; the limits are simple start values.
- The list of stations with their own time zone (`STATION_TIMEZONE_OVERRIDE`) is empty and was not checked
  against all station names.
- "Inside Brazil" is a box, not the real border.
- When the columns of a table change and only some years are run, the other years are gone until a run
  with all years.
- The reconcile, checks and quality report each read the data again, which makes the run slower (see S13).
