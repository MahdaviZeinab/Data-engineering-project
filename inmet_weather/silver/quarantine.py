"""
Silver 9: put aside the rows we can not use.

Every row gets ONE reason in the column dq_issue, or no reason (= a good row).
We check the reasons in this order and take the first one that is true:

  bad_timestamp      the date or the hour is wrong, so we don't know when it was measured
  unknown_station    the station is not in bronze.inmet_files (no name, state, place)
  unknown_timezone   the station's state has no time zone, so no local day
  not_in_brazil      the station is outside Brazil (good data, but not for our question)
  all_values_empty   all 17 measurements are empty, so the row has nothing to use

Good rows go to silver.inmet_hourly, the other rows to silver.inmet_quarantine.
Nothing is thrown away, so later we can check:
  Bronze rows = good rows + quarantined rows + removed duplicates   (Silver 10)
"""

from pyspark.sql import functions as F

from silver import contract
from silver.stations import to_number


def station_info(files):
    # One row per station and year, from bronze.inmet_files:
    # we know this station (known_station) and is it inside Brazil (in_brazil)?
    lat_min, lat_max = contract.BRAZIL_BOX["lat"]
    lon_min, lon_max = contract.BRAZIL_BOX["lon"]
    inside = (to_number("latitude").between(lat_min, lat_max)
              & to_number("longitude").between(lon_min, lon_max))
    return (files.select("station_code", "source_year",
                         F.lit(True).alias("known_station"),
                         F.coalesce(inside, F.lit(False)).alias("in_brazil"))
                 .dropDuplicates(["station_code", "source_year"]))


def find_reason(df):
    # The reason why we can't use this row (the first one that is true), or null.
    all_empty = F.lit(True)
    for column in contract.MEASUREMENTS:
        all_empty = all_empty & F.col(column).isNull()

    return (F.when(F.col("observed_at_utc").isNull(), "bad_timestamp")
             .when(~F.col("known_station"), "unknown_station")
             .when(F.col("timezone").isNull(), "unknown_timezone")
             .when(~F.col("in_brazil"), "not_in_brazil")
             .when(all_empty, "all_values_empty"))


def quarantine_rows(df, files):
    # Split the rows in two: (good rows, quarantined rows with their reason).
    df = df.join(F.broadcast(station_info(files)), ["station_code", "source_year"], "left")
    df = df.withColumn("known_station", F.coalesce("known_station", F.lit(False)))
    df = df.withColumn("dq_issue", find_reason(df)).drop("known_station", "in_brazil")

    good = df.where(F.col("dq_issue").isNull()).drop("dq_issue")
    quarantined = df.where(F.col("dq_issue").isNotNull())
    return good, quarantined


def count_reasons(quarantined):
    # How many rows per year and reason. Used in the run log and the quality report (S11).
    return (quarantined.groupBy("source_year", "dq_issue").count()
                       .orderBy("source_year", "dq_issue"))
