"""
A short report on the GitHub Actions run page (the "Summary" tab of a run).

GitHub gives every step a file, named in the environment variable GITHUB_STEP_SUMMARY.
Markdown written to that file is shown on the run page, so we can see what happened
(which year, which table, how many rows were invalid) without reading the whole log.
Outside GitHub (on a laptop, in the tests) the text is only printed.

  add_to_summary(text)              add Markdown text to the run page (and print it)
  markdown_table(header, rows)      make a Markdown table from a list of rows
  percent(part, total)              part / total in %, rounded (0 when total is 0)
  check_invalid_share(...)          a WARNING when more than MAX_INVALID_PCT % of the rows
                                    of a year are invalid (the job does not stop)
  report_failure(job, error)        show the error of a failed job on the run page
"""

import os

MAX_INVALID_PCT = 25        # more invalid rows than this (in %) gives a warning
MAX_ERROR_CHARS = 2000      # Spark errors can be very long; show only the start


def add_to_summary(text):
    # Print the text in the log, and add it to the run page when we run in GitHub Actions.
    print(text)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as summary:
            summary.write(text + "\n\n")


def markdown_table(header, rows):
    # header = ["year", "rows"], rows = [(2025, 100), ...]  ->  a Markdown table as text
    lines = ["| " + " | ".join(header) + " |",
             "|" + " --- |" * len(header)]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def percent(part, total):
    # 25, 100 -> 25.0   (0.0 when there is nothing to divide by)
    return round(100.0 * part / total, 2) if total else 0.0


def check_invalid_share(job, table, year, invalid_pct):
    # Returns the text for the "status" column of the report.
    # Above the limit it also prints a GitHub warning: a yellow box on the run page.
    # Only a warning: the job goes on and the tables are saved.
    if invalid_pct <= MAX_INVALID_PCT:
        return "OK"
    print(f"::warning title={job} data quality::{table}, year {year}: "
          f"{invalid_pct} % of the rows are invalid (limit {MAX_INVALID_PCT} %)")
    return f"WARNING: over {MAX_INVALID_PCT} %"


def report_failure(job, error):
    # Show the error on the run page (and as a red box), so we see at once what failed.
    text = str(error) or type(error).__name__
    first_line = text.splitlines()[0]
    print(f"::error title={job} failed::{first_line}")
    add_to_summary(f"### {job}: FAILED\n\n```\n{text[:MAX_ERROR_CHARS]}\n```")
