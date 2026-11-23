"""
Gold contract for the fire-risk use case: every rule and threshold in one place.

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
