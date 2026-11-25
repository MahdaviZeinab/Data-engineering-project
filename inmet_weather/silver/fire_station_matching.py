"""
Match every fire to its nearest INMET station.

To avoid comparing every fire with every station, the map is split into
GRID_DEG cells. Each station is placed in its own cell and the 8 around it,
so a fire only needs the stations in its own cell. Distance is haversine;
stations further than MAX_DISTANCE_KM are ignored.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver import fire_contract

EARTH_RADIUS_KM = 6371.0


def grid_cell(lat, lon):
    size = fire_contract.GRID_DEG
    return F.floor(lat / size).cast("int"), F.floor(lon / size).cast("int")


def distance_km(lat1, lon1, lat2, lon2):
    # haversine distance
    lat1, lon1, lat2, lon2 = [F.radians(c) for c in (lat1, lon1, lat2, lon2)]
    a = (F.pow(F.sin((lat2 - lat1) / 2), 2)
         + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2))
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def stations_by_cell(history):
    # one row per station, year and cell (own cell + 8 neighbours)
    stations = history.where(F.col("in_brazil")).select(
        "station_code", "source_year",
        F.col("latitude").alias("station_lat"), F.col("longitude").alias("station_lon"))
    cell_lat, cell_lon = grid_cell(F.col("station_lat"), F.col("station_lon"))
    stations = stations.withColumn("cell_lat", F.explode(F.array(cell_lat - 1, cell_lat, cell_lat + 1)))
    return stations.withColumn("cell_lon", F.explode(F.array(cell_lon - 1, cell_lon, cell_lon + 1)))


def add_nearest_station(fires, station_history):
    # Adds nearest_station_code and distance_km. Only stations active in the
    # fire's year are used.
    cell_lat, cell_lon = grid_cell(F.col("lat"), F.col("lon"))
    fires = (fires.withColumn("fire_row", F.monotonically_increasing_id())
                  .withColumn("cell_lat", cell_lat)
                  .withColumn("cell_lon", cell_lon))

    # ~600 stations x 9 cells per year, small enough to broadcast
    stations = F.broadcast(stations_by_cell(station_history))
    df = fires.join(stations, ["source_year", "cell_lat", "cell_lon"], "left")
    df = df.withColumn("distance_km", distance_km("lat", "lon", "station_lat", "station_lon"))

    too_far = F.col("distance_km") > fire_contract.MAX_DISTANCE_KM
    df = df.withColumn("station_code", F.when(~too_far, F.col("station_code")))
    df = df.withColumn("distance_km", F.when(~too_far, F.round("distance_km", 2)))

    # closest first; ties go to the lowest station code
    closest = Window.partitionBy("fire_row").orderBy(
        F.col("distance_km").asc_nulls_last(), F.col("station_code").asc_nulls_last())
    df = df.withColumn("rank", F.row_number().over(closest)).where(F.col("rank") == 1)
    df = df.withColumnRenamed("station_code", "nearest_station_code")
    return df.drop("rank", "fire_row", "cell_lat", "cell_lon", "station_lat", "station_lon")


def mark_no_station(df):
    # only set when the row has no other problem yet
    no_station = F.col("dq_issue").isNull() & F.col("nearest_station_code").isNull()
    return df.withColumn("dq_issue", F.when(no_station, "no_station_nearby")
                                      .otherwise(F.col("dq_issue")))
