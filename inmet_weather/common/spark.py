"""
Start Spark for our pipeline.

Every layer (Bronze, Silver, Gold) starts Spark with this function, so they
all see the same Spark tables. Spark remembers the tables in two places,
both inside the lake folder:

  <lake>/warehouse/      the table data (Parquet files, managed by Spark)
  <lake>/metastore_db/   the list of tables (Spark's "catalog")

Because both stay in the lake folder, the tables are still there the next
time we start Spark, and the next layer can read them by name:
    spark.table("bronze.inmet_hourly")
"""
from pyspark.sql import SparkSession


def start_spark(app_name, lake, memory="4g"):
    # Start Spark and tell it where to keep the tables and the table list.
    # enableHiveSupport() makes Spark save the table list to disk, so the
    # tables are not forgotten when Spark stops.
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.driver.memory", memory)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.warehouse.dir", f"{lake}/warehouse")
             .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                     f"jdbc:derby:;databaseName={lake}/metastore_db;create=true")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
