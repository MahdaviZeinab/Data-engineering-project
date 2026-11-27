"""
Group fire points that belong to the same fire.

A large fire shows up as several neighbouring points (one per satellite pixel).
Points on the same local day in the same ~1 km cell (EVENT_GRID_DEG) form one event.
Every point keeps its row and gets:
  fire_event_id    foco_id of the first point in the event
  points_in_event  number of points in the event
  first_in_event   True for exactly one point per event
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver import fire_contract


def add_fire_events(df):
    size = fire_contract.EVENT_GRID_DEG
    df = df.withColumn("event_lat", F.floor(F.col("lat") / size))
    df = df.withColumn("event_lon", F.floor(F.col("lon") / size))

    event = Window.partitionBy("local_date", "event_lat", "event_lon")
    first_seen = event.orderBy("observed_at_utc", "foco_id")     # deterministic order

    df = df.withColumn("rank", F.row_number().over(first_seen))
    df = df.withColumn("fire_event_id", F.first("foco_id").over(first_seen))
    df = df.withColumn("points_in_event", F.count("*").over(event))
    df = df.withColumn("first_in_event", F.col("rank") == 1)
    return df.drop("rank", "event_lat", "event_lon")
