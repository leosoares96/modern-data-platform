from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from lakehouse.quality import (  # noqa: E402
    CheckResult,
    Freshness,
    InRange,
    MinRows,
    NotNull,
    QualityCheckError,
    Unique,
    assert_no_errors,
    evaluate,
)

SCHEMA = "id string, value double, ts timestamp"


def frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def one(results, name):
    return next(r for r in results if r.check_name == name)


def test_not_null_counts_the_missing_values(spark):
    now = datetime.now(timezone.utc)
    df = frame(spark, [("a", 1.0, now), (None, 2.0, now), (None, 3.0, now)])
    result = evaluate(df, "t", [NotNull("id")])[0]
    assert (result.observed, result.passed) == (2.0, False)


def test_not_null_passes_on_a_complete_column(spark):
    df = frame(spark, [("a", 1.0, datetime.now(timezone.utc))])
    assert evaluate(df, "t", [NotNull("id")])[0].passed


def test_unique_counts_duplicated_values(spark):
    now = datetime.now(timezone.utc)
    df = frame(spark, [("a", 1.0, now), ("a", 2.0, now), ("b", 3.0, now)])
    result = evaluate(df, "t", [Unique("id")])[0]
    assert (result.observed, result.passed) == (1.0, False)


def test_in_range_counts_values_outside_the_bounds(spark):
    now = datetime.now(timezone.utc)
    df = frame(spark, [("a", 5.0, now), ("b", 11.0, now), ("c", -1.0, now)])
    result = evaluate(df, "t", [InRange("value", 0, 10)])[0]
    assert (result.observed, result.passed) == (2.0, False)


def test_min_rows(spark):
    df = frame(spark, [("a", 1.0, datetime.now(timezone.utc))])
    assert evaluate(df, "t", [MinRows(1)])[0].passed
    assert not evaluate(df, "t", [MinRows(5)])[0].passed


def test_min_rows_fails_on_an_empty_table(spark):
    assert not evaluate(frame(spark, []), "t", [MinRows(1)])[0].passed


def test_freshness_distinguishes_recent_from_stale_data(spark):
    recent = frame(spark, [("a", 1.0, datetime.now(timezone.utc) - timedelta(minutes=1))])
    stale = frame(spark, [("a", 1.0, datetime.now(timezone.utc) - timedelta(hours=5))])
    check = Freshness("ts", max_age_minutes=60)
    assert evaluate(recent, "t", [check])[0].passed
    assert not evaluate(stale, "t", [check])[0].passed


def test_all_checks_are_computed_in_one_pass_and_named(spark):
    now = datetime.now(timezone.utc)
    df = frame(spark, [("a", 1.0, now), ("b", 2.0, now)])
    results = evaluate(df, "t", [NotNull("id"), Unique("id"), InRange("value", 0, 10), MinRows(2)])
    assert [r.check_name for r in results] == [
        "not_null:id",
        "unique:id",
        "in_range:value",
        "min_rows",
    ]
    assert all(r.passed for r in results)
    assert one(results, "min_rows").observed == 2.0


def failed(severity):
    return CheckResult("t", "c", severity, False, 1.0, "0")


def test_only_error_severity_failures_raise():
    assert_no_errors([failed("warn")])
    with pytest.raises(QualityCheckError):
        assert_no_errors([failed("warn"), failed("error")])
