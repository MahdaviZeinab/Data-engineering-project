"""
Find the nearest INMET weather station for every fire.

Comparing every fire with every station is slow (more than 100,000 fires).
So we cut the map into squares (a grid) of GRID_DEG degrees:
  1. every station is put in its own square and in the 8 squares around it
  2. every fire is joined only with the stations of its own square
  3. we compute the distance in km (haversine) and keep the closest station

A station closer than MAX_DISTANCE_KM is always in one of those 9 squares,
so nothing close is missed. A fire with no station that close gets null.

Rules (grid size, max distance) are in silver/fire_contract.py.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver import fire_contract

EARTH_RADIUS_KM = 6371.0


def grid_cell(lat, lon):
    # The square a point is in, e.g. lat -10.5 -> -11 with 1 degree squares.
    size = fire_contract.GRID_DEG
    return F.floor(lat / size).cast("int"), F.floor(lon / size).cast("int")


def distance_km(lat1, lon1, lat2, lon2):
    # Haversine: the distance between two points on the earth, in km.
    lat1, lon1, lat2, lon2 = [F.radians(c) for c in (lat1, lon1, lat2, lon2)]
    a = (F.pow(F.sin((lat2 - lat1) / 2), 2)
         + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2))
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def stations_by_cell(history):
    # Stations with good coordinates in Brazil, one row per station, year and square.
    # Every station gets its own square and the 8 around it.
    stations = history.where(F.col("in_brazil")).select(
        "station_code", "source_year",
        F.col("latitude").alias("station_lat"), F.col("longitude").alias("station_lon"))
    cell_lat, cell_lon = grid_cell(F.col("station_lat"), F.col("station_lon"))
    stations = stations.withColumn("cell_lat", F.explode(F.array(cell_lat - 1, cell_lat, cell_lat + 1)))
    return stations.withColumn("cell_lon", F.explode(F.array(cell_lon - 1, cell_lon, cell_lon + 1)))


def add_nearest_station(fires, station_history):
    # Add nearest_station_code and distance_km to every fire (one row per fire).
    # Only a station of the same year is used, so a station that was
    # closed (or not built yet) that year is not picked.
    cell_lat, cell_lon = grid_cell(F.col("lat"), F.col("lon"))
    fires = (fires.withColumn("fire_row", F.monotonically_increasing_id())
                  .withColumn("cell_lat", cell_lat)
                  .withColumn("cell_lon", cell_lon))

    # The station table is small (about 600 stations x 9 squares a year), so broadcast it.
    stations = F.broadcast(stations_by_cell(station_history))
    df = fires.join(stations, ["source_year", "cell_lat", "cell_lon"], "left")
    df = df.withColumn("distance_km", distance_km("lat", "lon", "station_lat", "station_lon"))

    # Too far away: forget the station, so the fire gets null.
    too_far = F.col("distance_km") > fire_contract.MAX_DISTANCE_KM
    df = df.withColumn("station_code", F.when(~too_far, F.col("station_code")))
    df = df.withColumn("distance_km", F.when(~too_far, F.round("distance_km", 2)))

    # Keep the closest station. Same distance: the lowest code, so every run gives the same.
    closest = Window.partitionBy("fire_row").orderBy(
        F.col("distance_km").asc_nulls_last(), F.col("station_code").asc_nulls_last())
    df = df.withColumn("rank", F.row_number().over(closest)).where(F.col("rank") == 1)
    df = df.withColumnRenamed("station_code", "nearest_station_code")
    return df.drop("rank", "fire_row", "cell_lat", "cell_lon", "station_lat", "station_lon")


def mark_no_station(df):
    # A fire without a station nearby can not be joined with the weather.
    # Only fires without an other problem get this one (dq_issue = first problem).
    no_station = F.col("dq_issue").isNull() & F.col("nearest_station_code").isNull()
    return df.withColumn("dq_issue", F.when(no_station, "no_station_nearby")
                                      .otherwise(F.col("dq_issue")))
