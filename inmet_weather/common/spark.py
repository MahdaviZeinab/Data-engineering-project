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


def save_to_staging(spark, table, name):
    # Step A of saving: write the new rows to a "<name>_new" table first.
    # The real table is not touched yet, so if the job crashes here
    # (for example out of memory), the old data is still fine.
    database = name.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    table.write.mode("overwrite").format("parquet").saveAsTable(f"{name}_new")


def put_rows_in_table(spark, name, rows_table, years=None):
    # Put the rows of rows_table (e.g. "<name>_new") into the real table.
    # - Tables with a source_year column are split into one part per year.
    # - years=None: replace the whole table.
    # - years=[2025]: delete only the 2025 part and add the new 2025 rows,
    #   the other years are not touched.
    # Running the same job twice gives the same result (no double rows).
    rows = spark.table(rows_table)
    writer = rows.write.format("parquet")
    if "source_year" in rows.columns:
        writer = writer.partitionBy("source_year")

    exists = spark.catalog.tableExists(name)
    same_columns = exists and set(spark.table(name).columns) == set(new_rows.columns)
    if years and exists and not same_columns:
        print(f"NOTE: the columns of {name} changed, so the whole table is replaced. "
              "Years you did not run are gone until you run all years again.")

    if years and same_columns:
        for year in years:
            spark.sql(f"ALTER TABLE {name} DROP IF EXISTS PARTITION (source_year={year})")
        writer.mode("append").saveAsTable(name)
    else:
        writer.mode("overwrite").saveAsTable(name)


def move_from_staging(spark, name, years=None):
    # Step B of saving: put the rows from "<name>_new" into the real table,
    # then delete "<name>_new".
    put_rows_in_table(spark, name, f"{name}_new", years)
    spark.sql(f"DROP TABLE {name}_new")


def make_backup(spark, name, years=None):
    # Copy the rows we are about to replace to "<name>_old",
    # so we can put them back if something goes wrong.
    spark.sql(f"DROP TABLE IF EXISTS {name}_old")
    if not spark.catalog.tableExists(name):
        return                      # a new table: nothing to keep
    old_rows = spark.table(name)
    if years and "source_year" in old_rows.columns:
        old_rows = old_rows.where(old_rows["source_year"].isin(years))
    old_rows.write.mode("overwrite").format("parquet").saveAsTable(f"{name}_old")


def restore_backup(spark, name, years=None):
    # Put the old rows from "<name>_old" back into the real table.
    if spark.catalog.tableExists(f"{name}_old"):
        put_rows_in_table(spark, name, f"{name}_old", years)
    else:
        spark.sql(f"DROP TABLE IF EXISTS {name}")   # the table was new: remove it


def move_all_from_staging(spark, names, years=None):
    # Step B for SEVERAL tables that belong together (e.g. the 3 Bronze tables).
    # All tables must change together, or none of them:
    #   1. keep a copy of the old rows of every table ("<name>_old")
    #   2. move the new rows into every table
    #   3. if one move fails, put the old rows back in ALL tables
    #   4. only when all moves worked, delete the copies
    # names can also be a dict {table: years} when the tables replace
    # different years, e.g. {"silver.inmet_hourly": [2025], "silver.station": None}.
    if not isinstance(names, dict):
        names = {name: years for name in names}
    for name, table_years in names.items():
        make_backup(spark, name, table_years)
    try:
        for name, table_years in names.items():
            move_from_staging(spark, name, table_years)
    except Exception:
        print("SAVE FAILED: putting the old rows back in", list(names))
        for name, table_years in names.items():
            restore_backup(spark, name, table_years)
        raise
    for name in names:
        spark.sql(f"DROP TABLE IF EXISTS {name}_old")


def save_table(spark, table, name, years=None):
    # Save a DataFrame as a Spark table, e.g. name = "silver.inmet_hourly":
    # first to "<name>_new" (step A), then into the real table (step B).
    save_to_staging(spark, table, name)
    move_from_staging(spark, name, years)
