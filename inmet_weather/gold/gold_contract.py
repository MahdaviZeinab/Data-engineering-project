"""
Gold contract: every rule and threshold of the Gold use cases in one place.
  1. fire risk from weather (gold.fire_weather_daily, gold.fire_probability_by_score)
  2. regions suitable for wind and solar energy (gold.renewable_potential_by_*)

Plain Python only (no Spark), so it is easy to read, test and change.
The question Gold answers:
  "When several weather factors are bad on the same day (dry, hot, windy,
   many days without rain), how likely is it that a state has fires that day?"
"""

# A station-day is used only if it has at least this many hours of data.
# With fewer hours the daily rain total and the daily minimum are not reliable.
MIN_HOURS = 18

# A day with at least this much rain (state average) is a "rain day":
# it stops the dry spell. Less rain counts as a dry day.
RAIN_DAY_MM = 1.0

# Risk score: each bad weather factor gives points. Total: 0 (no risk) to 6.
#   humidity_min_pct < 30          +1      (< 20: +1 more)
#   temp_max_c       > 30          +1
#   wind_max_ms      > 8.33        +1      (8.33 m/s = 30 km/h, gusts included)
#   days_without_rain > 10         +1      (> 30: +1 more)
# The first three are the "30-30-30" rule of thumb for fire weather.
HUMIDITY_LOW = 30
HUMIDITY_VERY_LOW = 20
TEMP_HIGH = 30
WIND_STRONG_MS = 8.33
DRY_DAYS = 10
DRY_DAYS_LONG = 30
MAX_SCORE = 6

# Gold tables
DAILY = "gold.fire_weather_daily"            # one row per state and local day
PROBABILITY = "gold.fire_probability_by_score"  # one row per state (and "ALL") and score
ALL_STATES = "ALL"                           # the row for the whole country in PROBABILITY


# ---------------------------------------------------------------------------
# Use case 2: which stations and states have a climate suitable for wind or solar power?
# We use international thresholds, not our own (sources in gold/README.md):
#   wind:  NREL / PNL wind power classes at 10 m (Elliott et al., 1986)
#   solar: World Bank PV potential classes (ESMAP, 2020), turned into daily radiation (GHI)
# ---------------------------------------------------------------------------
AIR_DENSITY = 1.225          # kg/m3, standard air; wind power density = 1/2 * density * speed^3

# Wind power class at 10 m (the height of the INMET anemometers):
# the lowest power density (W/m2) of each class. Below 100 W/m2 is class 1.
WIND_CLASS_LIMITS = {2: 100, 3: 150, 4: 200, 5: 250, 6: 300, 7: 400}
WIND_SUITABLE_CLASS = 3      # class 3 or higher is good enough for a wind farm

# Solar: mean daily radiation in kWh/m2. The World Bank gives PV output per day
# (excellent > 4.5, favourable 3.5-4.5 kWh/kWp); divided by a typical performance
# ratio of 0.8 this is about 5.6 and 4.4 kWh/m2 of radiation.
SOLAR_EXCELLENT = 5.6
SOLAR_FAVOURABLE = 4.4       # below this: "limited"

# A station-year gets a class only if it has data for at least 80 % of the hours
# (wind) and of the days (solar); otherwise its averages do not cover the whole year.
MIN_COVERAGE = 0.8

RENEWABLE_STATION = "gold.renewable_potential_by_station"   # one row per station and year
RENEWABLE_STATE = "gold.renewable_potential_by_state"       # one row per state and year
