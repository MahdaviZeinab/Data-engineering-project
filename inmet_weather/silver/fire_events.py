"""
Merge the same fire seen twice into one fire event.

A big fire is often seen as several points next to each other (one per satellite pixel).
Counting points would count one big fire many times.

So we cut the map into small squares (EVENT_GRID_DEG, about 1 km).
All fire points on the same local day in the same square are one fire event.
Every point keeps its own row (nothing is lost), and gets:
  fire_event_id    the foco_id of the first point of the event
  points_in_event  how many points the event has
  first_in_event   true for one point per event, so Gold can count events:
                   fires.where("first_in_event").count()
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver import fire_contract


def add_fire_events(df):
    # Add fire_event_id, points_in_event and first_in_event to every fire point.
    size = fire_contract.EVENT_GRID_DEG
    df = df.withColumn("event_lat", F.floor(F.col("lat") / size))
    df = df.withColumn("event_lon", F.floor(F.col("lon") / size))

    event = Window.partitionBy("local_date", "event_lat", "event_lon")
    first_seen = event.orderBy("observed_at_utc", "foco_id")     # same order every run

    df = df.withColumn("rank", F.row_number().over(first_seen))
    df = df.withColumn("fire_event_id", F.first("foco_id").over(first_seen))
    df = df.withColumn("points_in_event", F.count("*").over(event))
    df = df.withColumn("first_in_event", F.col("rank") == 1)
    return df.drop("rank", "event_lat", "event_lon")
