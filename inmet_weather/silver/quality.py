"""
Row reconciliation, checks and the quality report for Silver.

If reconcile() or check_silver() fails, the job stops before anything is saved.
"""

from pyspark.sql import functions as F

from common.checks import no_nulls, one_row_per, run_checks
from silver import inmet_contract as contract


def rows_per_year(df):
    return {r["source_year"]: r["count"] for r in df.groupBy("source_year").count().collect()}


# reconciliation
def reconcile(rows_in, rows_out, rows_quarantined, duplicates):
    # Per year: Bronze rows = good + quarantined + duplicates. Raises if not.
    result, problems = {}, []
    for year in sorted(rows_in):
        counts = {
            "rows_read_from_bronze": rows_in[year],
            "good_rows_saved_in_silver": rows_out.get(year, 0),
            "bad_rows_moved_to_quarantine": rows_quarantined.get(year, 0),
            "duplicate_rows_removed": duplicates.get(year, 0),
        }
        result[year] = counts
        accounted = (counts["good_rows_saved_in_silver"] + counts["bad_rows_moved_to_quarantine"]
                     + counts["duplicate_rows_removed"])
        if accounted != counts["rows_read_from_bronze"]:
            problems.append(f"{year}: {counts}")
    if problems:
        raise ValueError("Reconcile failed, rows were lost or added: " + "; ".join(problems))
    return result


def checksum(df):
    # Sum of row hashes. Columns starting with "_" (run ids, load times) are
    # skipped, so two runs on the same Bronze data give the same value.
    columns = sorted(c for c in df.columns if not c.startswith("_"))
    return df.agg(F.sum(F.hash(*columns)).alias("checksum")).first()["checksum"]


def check_silver(df):
    run_checks(df, [one_row_per(*contract.KEY), no_nulls("state", "timezone")])


# quality report (before -> after, one row per year)
def count_flags_starting_with(prefix):
    return F.size(F.filter("dq_flags", lambda flag: flag.startswith(prefix)))


def quality_report(bronze, good, counts):
    # bronze: raw Bronze rows, good: final Silver rows, counts: output of reconcile()
    spark = good.sparkSession

    # before: missing codes in Bronze
    no_value = sum((F.col(c).isNull() | F.trim(F.col(c)).isin(contract.MISSING_CODES)).cast("int")
                   for c in contract.RENAME)
    before = bronze.groupBy("source_year").agg(F.sum(no_value).alias("empty_values_in_bronze"))

    # after: nulls, invalid values (nulled) and unusual values (kept)
    empty = sum(F.col(c).isNull().cast("int") for c in contract.MEASUREMENTS)
    after = good.groupBy("source_year").agg(
        F.sum(empty).alias("empty_values_in_silver"),
        F.sum(count_flags_starting_with("invalid_")).alias("impossible_values_set_to_empty"),
        F.sum(count_flags_starting_with("unusual_")).alias("rare_values_kept_with_flag"),
    )

    rows = spark.createDataFrame(
        [(year, c["rows_read_from_bronze"], c["duplicate_rows_removed"],
          c["bad_rows_moved_to_quarantine"], c["good_rows_saved_in_silver"])
         for year, c in counts.items()],
        "source_year int, rows_read_from_bronze long, duplicate_rows_removed long, "
        "bad_rows_moved_to_quarantine long, good_rows_saved_in_silver long")
    report = rows.join(before, "source_year", "left").join(after, "source_year", "left")
    return report.fillna(0).orderBy("source_year")
