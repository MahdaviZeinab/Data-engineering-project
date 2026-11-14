"""Tests for the Silver contract (S2). Plain Python, no Spark needed."""
import pytest

from silver.contract import (BRONZE_COLUMNS, INVALID, MEASUREMENTS, STATE_TIMEZONE,
                             UNUSUAL, check_schema)


def test_17_measurements_with_unique_names():
    assert len(MEASUREMENTS) == 17
    assert len(set(MEASUREMENTS)) == 17


def test_rules_only_use_known_columns():
    assert set(INVALID) <= set(MEASUREMENTS)
    assert set(UNUSUAL) <= set(MEASUREMENTS)


def test_every_state_has_a_timezone():
    assert len(STATE_TIMEZONE) == 27          # 26 states + DF


def test_schema_ok():
    check_schema(list(BRONZE_COLUMNS))        # no error


def test_schema_missing_column():
    with pytest.raises(ValueError, match="Missing: \\['Data'\\]"):
        check_schema([c for c in BRONZE_COLUMNS if c != "Data"])


def test_schema_new_column():
    with pytest.raises(ValueError, match="New: \\['extra'\\]"):
        check_schema(BRONZE_COLUMNS + ["extra"])
