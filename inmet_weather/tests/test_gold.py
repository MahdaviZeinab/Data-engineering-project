"""
Tests for the Gold job (fire risk, wind and solar), with tiny fake Silver tables (no files).
Run:  python -m pytest -q tests/test_gold.py
"""
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold import gold_contract as rules
from gold.gold_build import (add_classes, add_days_without_rain, add_risk_score,
                             join_weather_and_fires, run_gold, run_renewable, save_gold,
                             save_renewable, state_fires, state_weather, station_days,
                             station_year_energy)

WEATHER_SCHEMA = ("station_code string, state string, local_date date, source_year int, "
                  "temp_max_c double, humidity_min_pct double, precip_mm double, "
                  "wind_speed_ms double, wind_gust_ms double")
FIRE_SCHEMA = "foco_id string, state string, local_date date, source_year int, dq_issue string"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.warehouse.dir", str(warehouse))
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def empty_lake(spark):
    spark.catalog.clearCache()            # Gold caches its table; forget it before dropping
    spark.sql("DROP DATABASE IF EXISTS silver CASCADE")
    spark.sql("DROP DATABASE IF EXISTS gold CASCADE")


def day_of_weather(station, state, day, hours=24, temp=25.0, humidity=60.0, rain=0.0, wind=2.0):
    # One station and one day: the same values every hour.
    return [(station, state, day, day.year, temp, humidity, rain, wind, wind) for _ in range(hours)]


def weather(spark, rows):
    return spark.createDataFrame(rows, WEATHER_SCHEMA)


def fires(spark, rows):
    return spark.createDataFrame(rows, FIRE_SCHEMA)


def by_date(df):
    return {r["local_date"]: r for r in df.collect()}


# Step 1: weather per day
def test_rain_is_added_and_short_days_are_dropped(spark):
    d = date(2025, 9, 1)
    rows = day_of_weather("A1", "MT", d, rain=1.0) + day_of_weather("A2", "MT", d, hours=10)
    days = station_days(weather(spark, rows)).collect()
    assert len(days) == 1                         # A2 has only 10 of 24 hours
    assert days[0]["rain_mm"] == 24.0             # 24 hours of 1 mm, a total, not an average


def test_state_value_is_the_average_of_its_stations(spark):
    d = date(2025, 9, 1)
    rows = day_of_weather("A1", "MT", d, temp=30.0) + day_of_weather("A2", "MT", d, temp=34.0)
    row = state_weather(station_days(weather(spark, rows))).first()
    assert (row["stations"], row["temp_max_c"]) == (2, 32.0)


# Steps 2 and 3: join, dry spell, score
def test_join_keeps_days_without_fires_and_days_without_weather(spark):
    w = state_weather(station_days(weather(spark, day_of_weather("A1", "MT", date(2025, 9, 1)))))
    f = state_fires(fires(spark, [("f1", "MT", date(2025, 9, 2), 2025, None),
                                  ("f2", "MT", date(2025, 9, 2), 2025, "outside_brazil")]))
    days = by_date(join_weather_and_fires(w, f))
    assert (days[date(2025, 9, 1)]["fires"], days[date(2025, 9, 1)]["has_fire"]) == (0, False)
    assert days[date(2025, 9, 2)]["fires"] == 1   # the fire with a quality problem is left out
    assert days[date(2025, 9, 2)]["weather_ok"] is False


def test_days_without_rain_count_up_and_reset(spark):
    rains = [5.0, 0.0, 0.0, 0.0, 2.0]             # rain, 3 dry days, rain
    rows = []
    for i, rain in enumerate(rains):
        rows += day_of_weather("A1", "MT", date(2025, 9, 1 + i), rain=rain / 24)
    daily = join_weather_and_fires(state_weather(station_days(weather(spark, rows))),
                                   state_fires(fires(spark, [])))
    counts = [r["days_without_rain"] for r in add_days_without_rain(daily).orderBy("local_date").collect()]
    assert counts == [0, 1, 2, 3, 0]


def test_risk_score_adds_the_bad_factors(spark):
    rows = (day_of_weather("A1", "MT", date(2025, 9, 1), temp=36.0, humidity=15.0, wind=9.0)
            + day_of_weather("A1", "PA", date(2025, 9, 1)))          # mild weather
    daily = join_weather_and_fires(state_weather(station_days(weather(spark, rows))),
                                   state_fires(fires(spark, [])))
    daily = add_risk_score(add_days_without_rain(daily).withColumn("days_without_rain", F.lit(31)))
    scores = {r["state"]: r["risk_score"] for r in daily.collect()}
    assert scores == {"MT": rules.MAX_SCORE, "PA": 2}   # PA: only the 31 dry days


# The whole job
def save_silver(spark, weather_rows, fire_rows):
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    weather(spark, weather_rows).write.saveAsTable("silver.inmet_hourly")
    fires(spark, fire_rows).write.saveAsTable("silver.fires")


def test_gold_saves_daily_table_and_probability(spark):
    hot, mild = dict(temp=36.0, humidity=15.0, wind=9.0), {}
    rows = (day_of_weather("A1", "MT", date(2025, 9, 1), **hot)
            + day_of_weather("A1", "MT", date(2025, 9, 2), **hot)
            + day_of_weather("A1", "MT", date(2025, 9, 3), **mild))
    fire_rows = [("f1", "MT", date(2025, 9, 1), 2025, None), ("f2", "MT", date(2025, 9, 1), 2025, None)]
    save_silver(spark, rows, fire_rows)

    save_gold(spark, run_gold(spark, [2025]), [2025])

    assert spark.table(rules.DAILY).count() == 3
    report = {r["risk_score"]: r for r in spark.table(rules.PROBABILITY)
              .where(f"state = '{rules.ALL_STATES}'").collect()}
    # two hot days (score 4: humidity x2, temp, wind), one of them with fires
    assert (report[4]["days"], report[4]["fire_days"], report[4]["fire_probability"]) == (2, 1, 0.5)
    assert report[0]["fire_probability"] == 0.0


def test_gold_stops_when_a_fire_would_be_lost(spark, monkeypatch):
    save_silver(spark, day_of_weather("A1", "MT", date(2025, 9, 1)),
                [("f1", "MT", date(2025, 9, 1), 2025, None)])
    daily = run_gold(spark, [2025]).withColumn("fires", F.lit(0))   # a bug that loses the fire
    with pytest.raises(ValueError, match="Gold counts 0 fires, Silver has 1"):
        save_gold(spark, daily, [2025])
    assert not spark.catalog.tableExists(rules.DAILY)               # nothing was saved


# ---------------------------------------------------------------------------
# Use case 2: wind and solar
# ---------------------------------------------------------------------------
ENERGY_SCHEMA = ("station_code string, state string, local_date date, source_year int, "
                 "wind_speed_ms double, radiation_kj_m2 double")


def full_year(spark, station, state, wind, radiation):
    # One station, every hour of 2025: the same wind all the time,
    # and radiation only in the 12 day hours (empty at night, like INMET).
    hours = spark.range(365 * 24)
    return hours.select(
        F.lit(station).alias("station_code"), F.lit(state).alias("state"),
        F.expr("date_add(DATE'2025-01-01', CAST(id DIV 24 AS INT))").alias("local_date"),
        F.lit(2025).alias("source_year"),
        F.lit(wind).cast("double").alias("wind_speed_ms"),
        F.when(F.col("id") % 24 < 12, F.lit(radiation)).cast("double").alias("radiation_kj_m2"))


def test_wind_power_and_daily_sun(spark):
    # one day: wind 4 m/s every hour; 12 hours of 1800 kJ/m2 = 21600 kJ/m2 = 6 kWh/m2
    d = date(2025, 9, 1)
    rows = [("A1", "BA", d, 2025, 4.0, 1800.0 if hour < 12 else None) for hour in range(24)]
    row = station_year_energy(spark.createDataFrame(rows, ENERGY_SCHEMA)).first()
    assert row["wind_hours"] == 24 and row["mean_wind_ms"] == 4.0
    assert row["wind_power_w_m2"] == 39.2              # 0.5 * 1.225 * 4^3
    assert row["solar_days"] == 1 and row["ghi_kwh_m2_day"] == 6.0
    assert row["wind_coverage"] == round(24 / 8760, 3)  # one day of a whole year: too little


def test_classes_use_the_international_limits(spark):
    rows = [("strong", 160.0, 5.8, 1.0, 1.0),   # wind class 3, excellent sun
            ("weak", 90.0, 4.0, 1.0, 1.0),      # wind class 1, limited sun
            ("short", 500.0, 6.0, 0.5, 0.5)]    # too little data: no class
    df = spark.createDataFrame(rows, "station_code string, wind_power_w_m2 double, "
                                     "ghi_kwh_m2_day double, wind_coverage double, "
                                     "solar_coverage double")
    result = {r["station_code"]: r for r in add_classes(df).collect()}
    assert result["strong"]["wind_class"] == 3 and result["strong"]["wind_suitable"]
    assert result["strong"]["solar_class"] == "excellent" and result["strong"]["solar_suitable"]
    assert result["weak"]["wind_class"] == 1 and not result["weak"]["wind_suitable"]
    assert result["weak"]["solar_class"] == "limited" and not result["weak"]["solar_suitable"]
    assert result["short"]["wind_class"] is None and result["short"]["solar_class"] is None


def test_renewable_tables_are_saved(spark):
    # two stations in one state for a whole year: one windy, one calm; both sunny
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    weather = (full_year(spark, "W1", "RN", 7.0, 1800.0)       # 0.5*1.225*343 = 210 W/m2 -> class 4
               .unionByName(full_year(spark, "C1", "RN", 2.0, 1800.0)))
    weather.write.saveAsTable("silver.inmet_hourly")
    spark.createDataFrame([("W1", "Windy", -5.0, -36.0), ("C1", "Calm", -5.5, -36.5)],
                          "station_code string, station_name string, latitude double, "
                          "longitude double").write.saveAsTable("silver.station")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    save_renewable(spark, run_renewable(spark, [2025]), [2025])

    stations = {r["station_code"]: r for r in spark.table(rules.RENEWABLE_STATION).collect()}
    assert stations["W1"]["wind_class"] == 4 and stations["W1"]["wind_suitable"]
    assert stations["C1"]["wind_class"] == 1 and not stations["C1"]["wind_suitable"]
    assert stations["W1"]["solar_class"] == "excellent"           # 6 kWh/m2 per day
    assert stations["W1"]["station_name"] == "Windy"
    state = spark.table(rules.RENEWABLE_STATE).first()
    assert state["state"] == "RN" and state["stations"] == 2
    assert state["wind_suitable_stations"] == 1 and state["solar_suitable_stations"] == 2

