"""
Gold layer, two use cases:
  1. how likely is a fire in a state on a day with bad fire weather?
  2. which stations and states have a climate suitable for wind or solar energy?

It joins the two Silver tables per state and local day:
  silver.inmet_hourly (weather, one row per station and hour)
  silver.fires        (clean fires, one row per fire focus)

Steps:
  1. weather: hours -> one row per station and day -> one row per state and day
  2. fires:   one row per fire focus -> number of fires per state and day
  3. join both per state and day, add days without rain and the risk score (0-6)
  4. report: per risk score, the share of days that had a fire (= fire probability)

Tables (database "gold"):
  gold.fire_weather_daily        one row per state and local day, split by year
  gold.fire_probability_by_score one row per state (and "ALL") and risk score
  gold.renewable_potential_by_station  one row per station and year (wind and solar class)
  gold.renewable_potential_by_state    one row per state and year

All rules and thresholds are in gold/gold_contract.py.

Run (from the inmet_weather folder):
  python -m gold.gold_build                  # all years, dev
  python -m gold.gold_build --years 2025     # only 2025
  python -m gold.gold_build --env prod       # production lake
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.checks import has_columns, not_empty, one_row_per, run_checks
from common.report import add_to_summary, markdown_table, report_failure
from common.spark import save_table, start_spark
from gold import gold_contract as rules

KEY = ["state", "local_date"]


# Step 0: read Silver
def read_weather(spark, years=None):
    # Read silver.inmet_hourly. If years are given, keep only those years.
    # We keep only rows whose local day is in the same year as their file:
    # the first UTC hours of 1 January are still 31 December in Brazil, and
    # would otherwise put a few rows of the year before into this year's run.
    weather = spark.table("silver.inmet_hourly")
    if years:
        weather = weather.where(F.col("source_year").isin(years))
    return weather.where(F.year("local_date") == F.col("source_year"))


def read_silver(spark, years=None):
    # Read the two Silver tables (weather and fires), with the same rules.
    fires = spark.table("silver.fires")
    if years:
        fires = fires.where(F.col("source_year").isin(years))
    fires = fires.where(F.year("local_date") == F.col("source_year"))
    return read_weather(spark, years), fires


# Step 1: weather per state and day
def station_days(weather):
    # One row per station and day. Rain is ADDED over the hours (it is a total),
    # the other values take the day's maximum or minimum.
    # A day with too few hours is dropped: its rain total would be too small.
    days = weather.groupBy("station_code", "state", "local_date", "source_year").agg(
        F.count("*").alias("hours"),
        F.max("temp_max_c").alias("temp_max_c"),
        F.min("humidity_min_pct").alias("humidity_min_pct"),
        F.sum("precip_mm").alias("rain_mm"),
        F.greatest(F.max("wind_speed_ms"), F.max("wind_gust_ms")).alias("wind_max_ms"),
    )
    return days.where(F.col("hours") >= rules.MIN_HOURS)


def state_weather(days):
    # One row per state and day: the average of the state's stations.
    return days.groupBy("state", "local_date", "source_year").agg(
        F.count("*").alias("stations"),
        F.round(F.avg("temp_max_c"), 2).alias("temp_max_c"),
        F.round(F.avg("humidity_min_pct"), 2).alias("humidity_min_pct"),
        F.round(F.avg("rain_mm"), 2).alias("rain_mm"),
        F.round(F.avg("wind_max_ms"), 2).alias("wind_max_ms"),
    )


# Step 2: fires per state and day
def state_fires(fires):
    # Number of fire foci per state and day. Rows with a quality problem are left out.
    clean = fires.where(F.col("dq_issue").isNull() & F.col("state").isNotNull())
    return clean.groupBy("state", "local_date", "source_year").agg(F.count("*").alias("fires"))


# Step 3: join, days without rain, risk score
def join_weather_and_fires(weather_days, fire_days):
    # Full join: a day with weather but no fire gets fires = 0,
    # a day with fires but no weather is kept with weather_ok = false.
    daily = weather_days.join(fire_days, ["state", "local_date", "source_year"], "full")
    daily = daily.fillna(0, ["fires", "stations"])
    daily = daily.withColumn("has_fire", F.col("fires") > 0)
    return daily.withColumn("weather_ok", F.col("stations") > 0)


def add_days_without_rain(daily):
    # Count the days since the state's last rain day (0 on a rain day).
    # Trick: every rain day starts a new group; inside a group we count the days.
    # Days without weather are not rain days, so the count goes on through them.
    by_date = Window.partitionBy("state").orderBy("local_date")
    rain_day = F.coalesce(F.col("rain_mm") >= rules.RAIN_DAY_MM, F.lit(False))
    daily = daily.withColumn("rain_group", F.sum(rain_day.cast("int")).over(
        by_date.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    in_group = Window.partitionBy("state", "rain_group").orderBy("local_date")
    daily = daily.withColumn("days_without_rain", F.row_number().over(in_group) - 1)
    return daily.drop("rain_group")


def points(condition, value=1):
    # value points when the condition is true, 0 when it is false or unknown.
    return F.when(condition, value).otherwise(0)


def add_risk_score(daily):
    # Add the bad-weather points (rules in gold_contract.py). Score 0 (no risk) to 6.
    # No weather that day -> no score (null), the day is left out of the report.
    score = (points(F.col("humidity_min_pct") < rules.HUMIDITY_LOW)
             + points(F.col("humidity_min_pct") < rules.HUMIDITY_VERY_LOW)
             + points(F.col("temp_max_c") > rules.TEMP_HIGH)
             + points(F.col("wind_max_ms") > rules.WIND_STRONG_MS)
             + points(F.col("days_without_rain") > rules.DRY_DAYS)
             + points(F.col("days_without_rain") > rules.DRY_DAYS_LONG))
    return daily.withColumn("risk_score", F.when(F.col("weather_ok"), score))


# Step 4: fire probability per risk score
def probability_by_score(daily):
    # For each state and risk score: how many days, how many had a fire,
    # and the share of days with a fire. "ALL" = all states together.
    days = daily.where(F.col("weather_ok"))
    measures = [
        F.count("*").alias("days"),
        F.sum(F.col("has_fire").cast("int")).alias("fire_days"),
        F.sum("fires").alias("fires"),
    ]
    per_state = days.groupBy("state", "risk_score").agg(*measures)
    country = days.groupBy("risk_score").agg(*measures).withColumn("state", F.lit(rules.ALL_STATES))
    report = per_state.unionByName(country)
    report = report.withColumn("fire_probability", F.round(F.col("fire_days") / F.col("days"), 3))
    report = report.withColumn("fires_per_day", F.round(F.col("fires") / F.col("days"), 2))
    return report.orderBy("state", "risk_score")


# The whole job
def run_gold(spark, years=None, run_id=None):
    # Do steps 1-3 and return the daily table (not saved yet).
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    weather, fires = read_silver(spark, years)
    daily = join_weather_and_fires(state_weather(station_days(weather)), state_fires(fires))
    daily = add_risk_score(add_days_without_rain(daily))
    return daily.withColumn("_gold_run_id", F.lit(run_id)).cache()   # checked, then saved


def check_fire_total(spark, daily, years=None):
    # Every clean fire in Silver must be counted once in Gold (no fire lost or doubled).
    _, fires = read_silver(spark, years)
    expected = fires.where(F.col("dq_issue").isNull() & F.col("state").isNotNull()).count()
    counted = daily.agg(F.sum("fires")).first()[0] or 0
    if counted != expected:
        raise ValueError(f"Gold counts {counted} fires, Silver has {expected}; nothing was saved")


def save_gold(spark, daily, years=None):
    # Check the daily table, save it (only the years we ran),
    # then rebuild the small report from ALL years in the saved table.
    run_checks(daily, [not_empty, has_columns(*KEY, "fires", "risk_score"), one_row_per(*KEY)])
    check_fire_total(spark, daily, years)
    save_table(spark, daily, rules.DAILY, years)
    report = probability_by_score(spark.table(rules.DAILY))
    save_table(spark, report, rules.PROBABILITY)


def print_summary(spark):
    # Print the country-wide result in the log: fire probability per risk score.
    print("GOLD: fire probability per risk score, all states")
    (spark.table(rules.PROBABILITY).where(F.col("state") == rules.ALL_STATES)
          .orderBy("risk_score").show())


def write_report(spark, years=None):
    # The Gold result per year on the GitHub run page (see common/report.py).
    daily = spark.table(rules.DAILY)
    if years:
        daily = daily.where(F.col("source_year").isin(years))
    rows = (daily.groupBy("source_year").agg(
                F.count("*").alias("state_days"),
                F.sum(F.col("weather_ok").cast("int")).alias("with_weather"),
                F.sum(F.col("has_fire").cast("int")).alias("with_fire"),
                F.sum("fires").alias("fires"))
            .orderBy("source_year").collect())
    add_to_summary("### Gold\n\n" + markdown_table(
        ["year", "state-days", "with weather", "with fire", "fires"], [tuple(r) for r in rows]))


# ---------------------------------------------------------------------------
# Use case 2: regions suitable for wind and solar energy
# ---------------------------------------------------------------------------
def station_year_energy(weather):
    # One row per station and year: wind and sun measures and how complete the data is.
    # Wind: mean speed, and power density = 1/2 * air density * mean(speed^3).
    #       The energy grows with the CUBE of the speed, so windy hours count more.
    # Sun:  the hourly radiation is added up per day (kJ/m2 / 3600 = kWh/m2);
    #       a day counts only with enough hours (like station_days), then we average the days.
    keys = ["station_code", "state", "source_year"]
    wind = weather.where(F.col("wind_speed_ms").isNotNull()).groupBy(*keys).agg(
        F.count("*").alias("wind_hours"),
        F.round(F.avg("wind_speed_ms"), 2).alias("mean_wind_ms"),
        F.round(0.5 * rules.AIR_DENSITY * F.avg(F.pow("wind_speed_ms", 3)), 1)
         .alias("wind_power_w_m2"))
    days = weather.groupBy(*keys, "local_date").agg(
        F.count("*").alias("hours"),
        (F.sum("radiation_kj_m2") / 3600).alias("ghi_kwh_m2"))
    sun = (days.where((F.col("hours") >= rules.MIN_HOURS) & F.col("ghi_kwh_m2").isNotNull())
           .groupBy(*keys).agg(F.count("*").alias("solar_days"),
                               F.round(F.avg("ghi_kwh_m2"), 2).alias("ghi_kwh_m2_day")))

    # A station with wind data but no radiation (or the other way round) is kept.
    df = wind.join(sun, keys, "full").fillna(0, ["wind_hours", "solar_days"])
    days_in_year = F.expr("dayofyear(make_date(source_year, 12, 31))")     # 365 or 366
    df = df.withColumn("wind_coverage", F.round(F.col("wind_hours") / (days_in_year * 24), 3))
    return df.withColumn("solar_coverage", F.round(F.col("solar_days") / days_in_year, 3))


def wind_class(power):
    # NREL wind power class (1-7) from the power density at 10 m.
    result = F.lit(1)
    for number, limit in sorted(rules.WIND_CLASS_LIMITS.items()):
        result = F.when(power >= limit, number).otherwise(result)
    return result


def solar_class(ghi):
    # World Bank class from the mean daily radiation: excellent, favourable or limited.
    return (F.when(ghi >= rules.SOLAR_EXCELLENT, "excellent")
             .when(ghi >= rules.SOLAR_FAVOURABLE, "favourable")
             .otherwise("limited"))


def add_classes(df):
    # The classes, only when the station-year has enough data; otherwise they stay empty.
    enough_wind = F.col("wind_coverage") >= rules.MIN_COVERAGE
    enough_sun = F.col("solar_coverage") >= rules.MIN_COVERAGE
    df = df.withColumn("wind_class", F.when(enough_wind, wind_class(F.col("wind_power_w_m2"))))
    df = df.withColumn("wind_suitable", F.col("wind_class") >= rules.WIND_SUITABLE_CLASS)
    df = df.withColumn("solar_class", F.when(enough_sun, solar_class(F.col("ghi_kwh_m2_day"))))
    return df.withColumn("solar_suitable", F.col("solar_class") != "limited")


def add_location(spark, df):
    # Name and position of each station (for a map), from silver.station.
    stations = spark.table("silver.station").select("station_code", "station_name",
                                                    "latitude", "longitude")
    return df.join(F.broadcast(stations), "station_code", "left")


def renewable_by_state(stations):
    # One row per state and year: how many stations are suitable, and the averages
    # of the stations with enough data. The state solar class uses the same limits.
    with_wind = F.col("wind_class").isNotNull()
    with_sun = F.col("solar_class").isNotNull()
    df = stations.groupBy("state", "source_year").agg(
        F.count("*").alias("stations"),
        F.sum(with_wind.cast("int")).alias("stations_with_wind_data"),
        F.sum(F.col("wind_suitable").cast("int")).alias("wind_suitable_stations"),
        F.max("wind_class").alias("best_wind_class"),
        F.round(F.avg(F.when(with_wind, F.col("mean_wind_ms"))), 2).alias("mean_wind_ms"),
        F.sum(with_sun.cast("int")).alias("stations_with_solar_data"),
        F.sum(F.col("solar_suitable").cast("int")).alias("solar_suitable_stations"),
        F.round(F.avg(F.when(with_sun, F.col("ghi_kwh_m2_day"))), 2).alias("ghi_kwh_m2_day"))
    df = df.fillna(0, ["wind_suitable_stations", "solar_suitable_stations"])
    df = df.withColumn("solar_class", F.when(F.col("ghi_kwh_m2_day").isNotNull(),
                                             solar_class(F.col("ghi_kwh_m2_day"))))
    return df.orderBy("state", "source_year")


def run_renewable(spark, years=None, run_id=None):
    # Build the station table (not saved yet).
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df = add_classes(station_year_energy(read_weather(spark, years)))
    df = add_location(spark, df)
    return df.withColumn("_gold_run_id", F.lit(run_id)).cache()      # checked, then saved


def save_renewable(spark, stations, years=None):
    # Check the station table, save it (only the years we ran),
    # then rebuild the state table from ALL years in the saved table.
    run_checks(stations, [not_empty,
                          has_columns("station_code", "source_year", "wind_class", "solar_class"),
                          one_row_per("station_code", "source_year")])
    save_table(spark, stations, rules.RENEWABLE_STATION, years)
    save_table(spark, renewable_by_state(spark.table(rules.RENEWABLE_STATION)), rules.RENEWABLE_STATE)


def write_renewable_report(spark, years=None):
    # Wind and solar per state on the GitHub run page (see common/report.py).
    states = spark.table(rules.RENEWABLE_STATE)
    if years:
        states = states.where(F.col("source_year").isin(years))
    rows = [(r["state"], r["source_year"], r["stations"],
             f"{r['wind_suitable_stations']} / {r['stations_with_wind_data']}",
             r["best_wind_class"], r["mean_wind_ms"],
             f"{r['solar_suitable_stations']} / {r['stations_with_solar_data']}",
             r["ghi_kwh_m2_day"], r["solar_class"])
            for r in states.orderBy("source_year", "state").collect()]
    add_to_summary("### Gold: wind and solar potential per state\n\n" + markdown_table(
        ["state", "year", "stations", "wind suitable", "best wind class", "mean wind m/s",
         "solar suitable", "GHI kWh/m2/day", "solar class"], rows))


def main():
    # Read the command line options, start Spark, build Gold and save it.
    parser = argparse.ArgumentParser(description="Build the Gold tables (fire risk, wind and solar) from Silver.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--years", type=int, nargs="*")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("gold", lake, memory="2g")
    try:
        daily = run_gold(spark, args.years)
        save_gold(spark, daily, args.years)
        print(f"GOLD: saved {rules.DAILY} and {rules.PROBABILITY}")
        print_summary(spark)
        write_report(spark, args.years)        # the result on the GitHub run page

        stations = run_renewable(spark, args.years)
        save_renewable(spark, stations, args.years)
        print(f"GOLD: saved {rules.RENEWABLE_STATION} and {rules.RENEWABLE_STATE}")
        write_renewable_report(spark, args.years)
        return 0
    except Exception as error:
        print("GOLD FAILED:", error)
        report_failure("Gold", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
