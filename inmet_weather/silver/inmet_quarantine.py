"""
Move unusable INMET rows to silver.inmet_quarantine.

Each row gets the first matching reason in dq_issue (null = good row):

  bad_timestamp      date or hour can't be parsed
  unknown_station    station not in bronze.inmet_files
  unknown_timezone   no time zone for the station's state
  not_in_brazil      station coordinates outside Brazil
  all_values_empty   all 17 measurements are null

Nothing is dropped: Bronze rows = good + quarantined + removed duplicates.
"""

from pyspark.sql import functions as F

from silver import inmet_contract as contract
from silver.inmet_stations import to_number


def station_info(files):
    # one row per station-year: known_station and in_brazil
    lat_min, lat_max = contract.BRAZIL_BOX["lat"]
    lon_min, lon_max = contract.BRAZIL_BOX["lon"]
    inside = (to_number("latitude").between(lat_min, lat_max)
              & to_number("longitude").between(lon_min, lon_max))
    return (files.select("station_code", "source_year",
                         F.lit(True).alias("known_station"),
                         F.coalesce(inside, F.lit(False)).alias("in_brazil"))
                 .dropDuplicates(["station_code", "source_year"]))


def find_reason(df):
    all_empty = F.lit(True)
    for column in contract.MEASUREMENTS:
        all_empty = all_empty & F.col(column).isNull()

    return (F.when(F.col("observed_at_utc").isNull(), "bad_timestamp")
             .when(~F.col("known_station"), "unknown_station")
             .when(F.col("timezone").isNull(), "unknown_timezone")
             .when(~F.col("in_brazil"), "not_in_brazil")
             .when(all_empty, "all_values_empty"))


def quarantine_rows(df, files):
    # returns (good rows, quarantined rows)
    df = df.join(F.broadcast(station_info(files)), ["station_code", "source_year"], "left")
    df = df.withColumn("known_station", F.coalesce("known_station", F.lit(False)))
    df = df.withColumn("dq_issue", find_reason(df)).drop("known_station", "in_brazil")

    good = df.where(F.col("dq_issue").isNull()).drop("dq_issue")
    quarantined = df.where(F.col("dq_issue").isNotNull())
    return good, quarantined


def count_reasons(quarantined):
    return (quarantined.groupBy("source_year", "dq_issue").count()
                       .orderBy("source_year", "dq_issue"))
