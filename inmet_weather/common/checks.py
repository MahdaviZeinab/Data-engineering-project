"""
Small checks we run on a table before we save it.
Each check gets a DataFrame and stops the job (raises an error) if something is wrong,
so a bad table is never saved.
"""


def not_empty(df):
    # Stop if the table has no rows at all.
    if df.limit(1).count() == 0:
        raise ValueError("table is empty")


def has_columns(*columns):
    # Make a check that stops if one of these columns is missing.
    def check(df):
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"missing columns: {missing}")
    return check


def one_row_per(*key):
    # Make a check that stops if the key is not unique (two rows with the same key).
    def check(df):
        extra = df.count() - df.select(*key).distinct().count()
        if extra:
            raise ValueError(f"{extra} duplicate rows for key {list(key)}")
    return check


def no_nulls(*columns):
    # Make a check that stops if one of these columns has an empty value.
    def check(df):
        for column in columns:
            empty = df.where(f"{column} IS NULL").count()
            if empty:
                raise ValueError(f"{empty} rows with an empty {column}")
    return check


def run_checks(df, checks):
    # Run every check on the table, one after the other.
    for check in checks:
        check(df)
