"""
Tests for common/report.py (no Spark needed).
Run:  python -m pytest -q tests/test_report.py
"""
from common.report import (MAX_INVALID_PCT, add_to_summary, check_invalid_share,
                           markdown_table, percent, report_failure)


def test_percent():
    assert percent(25, 100) == 25.0
    assert percent(1, 3) == 33.33
    assert percent(5, 0) == 0.0                 # nothing read -> no division by zero


def test_markdown_table():
    text = markdown_table(["year", "rows"], [(2025, 10)])
    assert text.splitlines() == ["| year | rows |", "| --- | --- |", "| 2025 | 10 |"]


def test_warning_only_above_the_limit(capsys):
    assert check_invalid_share("Silver", "silver.t", 2025, MAX_INVALID_PCT) == "OK"
    assert "WARNING" in check_invalid_share("Silver", "silver.t", 2025, MAX_INVALID_PCT + 1)
    assert "::warning" in capsys.readouterr().out


def test_summary_goes_to_the_github_file(tmp_path, monkeypatch):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    add_to_summary("### Bronze")
    report_failure("Silver INMET", ValueError("no files found for 2023"))
    text = summary.read_text(encoding="utf-8")
    assert "### Bronze" in text
    assert "Silver INMET: FAILED" in text and "no files found for 2023" in text


def test_no_github_file_only_prints(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    add_to_summary("hello")
    assert "hello" in capsys.readouterr().out
