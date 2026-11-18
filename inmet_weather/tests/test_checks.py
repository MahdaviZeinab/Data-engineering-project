"""Tests for the small table checks and for save_table."""
import pytest
from pyspark.sql import SparkSession

from common.checks import has_columns, not_empty, one_row_per, run_checks
from common.spark import save_table


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def test_checks_stop_bad_tables(spark):
    df = spark.createDataFrame([(1,), (1,)], "x int")
    run_checks(df, [not_empty, has_columns("x")])                  # no error
    with pytest.raises(ValueError):
        run_checks(spark.createDataFrame([], "x int"), [not_empty])
    with pytest.raises(ValueError):
        run_checks(df, [has_columns("y")])
    with pytest.raises(ValueError):
        run_checks(df, [one_row_per("x")])


def test_save_table_replaces_whole_table(spark):
    save_table(spark, spark.createDataFrame([(1,)], "x int"), "test_db.t")
    save_table(spark, spark.createDataFrame([(1,), (2,)], "x int"), "test_db.t")
    assert spark.table("test_db.t").count() == 2


def test_save_table_replaces_only_given_years(spark):
    rows = spark.createDataFrame([("old", 2020), ("old", 2021)], "run string, source_year int")
    save_table(spark, rows, "test_db.years")
    new = spark.createDataFrame([("new", 2021)], "run string, source_year int")
    save_table(spark, new, "test_db.years", years=[2021])
    result = {r.source_year: r.run for r in spark.table("test_db.years").collect()}
    assert result == {2020: "old", 2021: "new"}
