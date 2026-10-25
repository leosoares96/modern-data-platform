"""Declarative data quality checks, evaluated in a single pass and persisted as a table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pyspark.sql import Column
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

RESULTS_TABLE = "lake.quality.check_results"


class QualityCheckError(Exception):
    """Raised when a check with severity `error` fails."""


@dataclass(frozen=True)
class CheckResult:
    table_name: str
    check_name: str
    severity: str
    passed: bool
    observed: float | None
    expected: str


class Check(Protocol):
    name: str
    severity: str

    def metric(self) -> Column: ...

    def judge(self, observed: float | None) -> tuple[bool, str]: ...


def _count_where(condition: Column) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


@dataclass(frozen=True)
class NotNull:
    column: str
    severity: str = "error"

    @property
    def name(self) -> str:
        return f"not_null:{self.column}"

    def metric(self) -> Column:
        return _count_where(F.col(self.column).isNull())

    def judge(self, observed: float | None) -> tuple[bool, str]:
        return observed == 0, "0 null values"


@dataclass(frozen=True)
class Unique:
    column: str
    severity: str = "error"

    @property
    def name(self) -> str:
        return f"unique:{self.column}"

    def metric(self) -> Column:
        return F.count(F.lit(1)) - F.countDistinct(self.column)

    def judge(self, observed: float | None) -> tuple[bool, str]:
        return observed == 0, "0 duplicated values"


@dataclass(frozen=True)
class InRange:
    column: str
    low: float
    high: float
    severity: str = "error"

    @property
    def name(self) -> str:
        return f"in_range:{self.column}"

    def metric(self) -> Column:
        return _count_where((F.col(self.column) < self.low) | (F.col(self.column) > self.high))

    def judge(self, observed: float | None) -> tuple[bool, str]:
        return observed == 0, f"0 values outside [{self.low}, {self.high}]"


@dataclass(frozen=True)
class MinRows:
    minimum: int
    severity: str = "error"

    @property
    def name(self) -> str:
        return "min_rows"

    def metric(self) -> Column:
        return F.count(F.lit(1))

    def judge(self, observed: float | None) -> tuple[bool, str]:
        return observed is not None and observed >= self.minimum, f">= {self.minimum} rows"


@dataclass(frozen=True)
class Freshness:
    """The newest value of a timestamp column must be at most `max_age_minutes` old."""

    column: str
    max_age_minutes: float
    severity: str = "warn"

    @property
    def name(self) -> str:
        return f"freshness:{self.column}"

    def metric(self) -> Column:
        newest = F.unix_timestamp(F.max(self.column))
        return (F.unix_timestamp(F.current_timestamp()) - newest) / 60.0

    def judge(self, observed: float | None) -> tuple[bool, str]:
        passed = observed is not None and observed <= self.max_age_minutes
        return passed, f"newest value at most {self.max_age_minutes} minutes old"


def evaluate(df: DataFrame, table_name: str, checks: list[Check]) -> list[CheckResult]:
    """Run every check against `df` with one aggregation."""
    row = df.agg(*[check.metric().alias(f"m{i}") for i, check in enumerate(checks)]).first()
    results = []
    for i, check in enumerate(checks):
        observed = row[f"m{i}"]
        passed, expected = check.judge(observed)
        results.append(
            CheckResult(
                table_name=table_name,
                check_name=check.name,
                severity=check.severity,
                passed=bool(passed),
                observed=None if observed is None else float(observed),
                expected=expected,
            )
        )
    return results


def persist_results(spark: SparkSession, run_id: str, job: str, results: list[CheckResult]) -> None:
    """Append the results to lake.quality.check_results."""
    rows = [
        (
            run_id,
            job,
            r.table_name,
            r.check_name,
            r.severity,
            r.passed,
            r.observed,
            r.expected,
        )
        for r in results
    ]
    schema = (
        "run_id string, job string, table_name string, check_name string, severity string, "
        "passed boolean, observed double, expected string"
    )
    frame = spark.createDataFrame(rows, schema).withColumn("checked_at", F.current_timestamp())
    frame.writeTo(RESULTS_TABLE).append()


def assert_no_errors(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed and r.severity == "error"]
    if failures:
        details = "; ".join(
            f"{r.table_name} {r.check_name} observed={r.observed} expected {r.expected}"
            for r in failures
        )
        raise QualityCheckError(f"{len(failures)} data quality check(s) failed: {details}")
