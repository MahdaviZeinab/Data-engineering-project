"""
S0: safe publish (write -> check -> swap).

A table is never half-written: new data goes to a staging folder first,
is checked, and only then replaces the current table. If a check fails,
the old table stays as it was.

    <layer>/_staging/<table>_<run_id>   new data, being checked
    <layer>/_previous/<table>           last good version (for rollback by hand)
    <layer>/<table>/_published.json     written last, says the table is READY
"""
import json
from datetime import datetime, timezone


def _fs(spark, path):
    hadoop = spark._jvm.org.apache.hadoop.fs
    return hadoop, hadoop.Path(path).getFileSystem(spark._jsc.hadoopConfiguration())


# Small checks. Each gets the staged DataFrame and raises if something is wrong.
def not_empty(df):
    if df.limit(1).count() == 0:
        raise ValueError("table is empty")


def has_columns(*columns):
    def check(df):
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"missing columns: {missing}")
    return check


def publish(spark, df, table_path, run_id, checks=(), partition_by=None):
    """Write df to table_path safely. Returns the rows per partition (or the total)."""
    layer, table = table_path.rstrip("/").rsplit("/", 1)
    staging = f"{layer}/_staging/{table}_{run_id}"
    previous = f"{layer}/_previous/{table}"
    hadoop, fs = _fs(spark, table_path)
    P = hadoop.Path

    # 1. write to staging (folders starting with "_" are ignored by Spark readers)
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(staging)

    # 2. read it back and check it. If a check fails, the old table is not touched.
    staged = spark.read.parquet(staging)
    try:
        for check in checks:
            check(staged)
    except Exception:
        fs.delete(P(staging), True)
        raise
    if partition_by:
        rows = {str(r[0]): r[1] for r in staged.groupBy(partition_by).count().orderBy(partition_by).collect()}
    else:
        rows = {"all": staged.count()}

    # 3. swap: current -> _previous, staging -> current (rename returns False on failure)
    fs.delete(P(previous), True)                 # keep only the last good version
    fs.mkdirs(P(f"{layer}/_previous"))
    had_old = fs.exists(P(table_path))
    if had_old and not fs.rename(P(table_path), P(previous)):
        raise RuntimeError(f"could not move {table_path} to {previous}; nothing changed")
    if not fs.rename(P(staging), P(table_path)):
        if had_old:
            fs.rename(P(previous), P(table_path))   # put the old one back
        raise RuntimeError(f"could not move {staging} to {table_path}; old table kept")

    # 4. only now mark the table as ready
    info = {"run_id": run_id, "published_at": datetime.now(timezone.utc).isoformat(),
            "rows": rows, "status": "READY"}
    out = fs.create(P(f"{table_path}/_published.json"), True)
    out.write(bytearray(json.dumps(info, indent=2), "utf-8"))
    out.close()
    return rows
