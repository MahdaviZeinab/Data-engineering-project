# Gold layer: fire risk from weather

**Question:** when several weather factors are bad on the same day (dry, hot, windy,
many days without rain), how likely is it that a state has fires that day?

**Consumer:** an environmental analyst or civil-defence planner who wants to know
which weather conditions come with fires, to plan warnings for the fire season.

## Tables

| Spark table | One row per | Content |
|---|---|---|
| `gold.fire_weather_daily` | state × local day | daily weather of the state, number of fires, `days_without_rain`, `risk_score` (0–6), split by `source_year` |
| `gold.fire_probability_by_score` | state × risk score (+ `ALL` = whole country) | `days`, `fire_days`, `fires`, `fire_probability` = fire_days / days, `fires_per_day` |

Gold is **saved** (not computed on demand): both sources arrive once a year, so a
saved table is just as fresh, it is checked before it is published, and the dashboard
reads a small table instead of millions of hourly rows.

## How it is built (`gold_build.py`, rules in `gold_contract.py`)

1. **Station-day:** hours → one row per station and local day. Rain is **added up**;
   temperature, humidity and wind take the day's max or min. A station-day needs at
   least `MIN_HOURS = 18` hours, otherwise its rain total is too small.
2. **State-day weather:** the average of the state's stations (`stations` = how many).
3. **State-day fires:** fire foci per state and local day (only rows without `dq_issue`).
4. **Join** on `state` + `local_date` (full join): no fire → `fires = 0`;
   fires but no weather → `weather_ok = false`, no score.
5. **Days without rain:** days since the last state-day with ≥ 1 mm rain.
6. **Risk score (0–6):**

| Factor | Points |
|---|---|
| min humidity < 30 % (< 20 %) | +1 (+1 more) |
| max temperature > 30 °C | +1 |
| max wind or gust > 8.33 m/s (30 km/h) | +1 |
| days without rain > 10 (> 30) | +1 (+1 more) |

7. **Report:** for each score, the share of days with at least one fire.

## Checks before saving
- one row per `state` + `local_date`
- every clean fire in Silver is counted once in Gold (totals must match), else nothing is saved
- the daily table replaces only the years we ran; the report is rebuilt from all years

## Limits
- One average per state: large states with few stations (e.g. Amazonas) are less reliable.
- `days_without_rain` starts at 0 at the first day of the data (in dev: 1 January 2025).
- The score shows a link, not a cause: the dry season is also the burning season.
- Fire foci are satellite detections, not fire events.

## Run
```bash
python -m gold.gold_build                  # all years, dev
python -m gold.gold_build --years 2025     # only 2025
python -m pytest -q tests/test_gold.py     # the Gold tests
```

## Dashboard: risk map (`state_month_risk.py`)

`gold.state_month_risk` has one row per state and month: `fire_count` (clean fires from
`silver.fires`), and `avg_humidity`, `avg_temp`, `total_rain` from `silver.inmet_hourly`.
Weather is averaged per station first, then over the state's stations, so `total_rain`
is the monthly rain of a typical station. The join starts from the fires: a state with
fires but no station is kept, with empty weather.

It feeds a map of Brazil: each state is coloured by `fire_count`, and the weather shows on hover.

```sql
SELECT state, fire_count, avg_humidity, avg_temp, total_rain
FROM gold.state_month_risk
WHERE source_year = 2025 AND month = 9
ORDER BY fire_count DESC;
```

```bash
python -m gold.state_month_risk --years 2025
python -m pytest -q tests/test_state_month_risk.py
```
