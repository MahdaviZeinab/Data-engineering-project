"""
Silver 10 and 11: prove that nothing is lost, and show what Silver changed.

  rows_per_year(df)    rows per year, for example {2024: 8760, 2025: 8000}
  reconcile(...)       S10: Bronze rows = Silver rows + quarantined rows + removed duplicates
  checksum(df)         S10: one number for the whole table; a rerun must give the same number
  check_silver(df)     S10: one row per station and hour, and every row has a state and time zone
  quality_report(...)  S11: one row per year with the "before -> after" numbers

If reconcile or check_silver fails, the job stops BEFORE saving, so the old tables stay.
"""

from pyspark.sql import functions as F

from common.checks import no_nulls, one_row_per, run_checks
from silver import contract


def rows_per_year(df):
    # {year: number of rows}
    return {r["source_year"]: r["count"] for r in df.groupBy("source_year").count().collect()}


# ---------------------------------------------------------------------------
# Silver 10: nothing is lost, and a rerun gives the same result
# ---------------------------------------------------------------------------
def reconcile(rows_in, rows_out, rows_quarantined, duplicates):
    # Every Bronze row must end up in exactly one place, for every year:
    #   rows_in = rows_out + rows_quarantined + duplicates_removed
    # Returns the numbers per year. Stops the job if one year does not add up.
    result, problems = {}, []
    for year in sorted(rows_in):
        counts = {
            "rows_in": rows_in[year],
            "rows_out": rows_out.get(year, 0),
            "rows_quarantined": rows_quarantined.get(year, 0),
            "duplicates_removed": duplicates.get(year, 0),
        }
        result[year] = counts
        accounted = counts["rows_out"] + counts["rows_quarantined"] + counts["duplicates_removed"]
        if accounted != counts["rows_in"]:
            problems.append(f"{year}: {counts}")
    if problems:
        raise ValueError("Reconcile failed, rows were lost or added: " + "; ".join(problems))
    return result


def checksum(df):
    # One number for the whole table: the sum of a hash of every row.
    # Columns that start with "_" (run ids, load times) are left out,
    # so two runs on the same Bronze data must give the same number.
    columns = sorted(c for c in df.columns if not c.startswith("_"))
    return df.agg(F.sum(F.hash(*columns)).alias("checksum")).first()["checksum"]


def check_silver(df):
    # The rules every good Silver row must follow.
    run_checks(df, [one_row_per(*contract.KEY), no_nulls("state", "timezone")])


# ---------------------------------------------------------------------------
# Silver 11: quality report ("before -> after"), one row per year
# ---------------------------------------------------------------------------
def count_flags_starting_with(prefix):
    # number of flags in dq_flags that start with prefix, for one row
    return F.size(F.filter("dq_flags", lambda flag: flag.startswith(prefix)))


def quality_report(bronze, good, counts):
    # bronze: the Bronze rows (text), good: the final Silver rows,
    # counts: the numbers from reconcile().
    spark = good.sparkSession

    # Before: INMET's "no value" codes in Bronze (empty or -9999)
    no_value = sum((F.col(c).isNull() | F.trim(F.col(c)).isin(contract.MISSING_CODES)).cast("int")
                   for c in contract.RENAME)
    before = bronze.groupBy("source_year").agg(F.sum(no_value).alias("missing_values_in"))

    # After: empty values, invalid values (made empty) and unusual values (kept) in Silver
    empty = sum(F.col(c).isNull().cast("int") for c in contract.MEASUREMENTS)
    after = good.groupBy("source_year").agg(
        F.sum(empty).alias("empty_values_out"),
        F.sum(count_flags_starting_with("invalid_")).alias("values_invalid"),
        F.sum(count_flags_starting_with("unusual_")).alias("values_unusual"),
    )

    rows = spark.createDataFrame(
        [(year, c["rows_in"], c["duplicates_removed"], c["rows_quarantined"], c["rows_out"])
         for year, c in counts.items()],
        "source_year int, rows_in long, duplicates_removed long, rows_quarantined long, rows_out long")
    report = rows.join(before, "source_year", "left").join(after, "source_year", "left")
    return report.fillna(0).orderBy("source_year")
