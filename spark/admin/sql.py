"""Run one SQL statement against the lakehouse and print the result.

  sql.py "SELECT airline, count(*) FROM lake.silver.flights GROUP BY airline"
"""

from __future__ import annotations

import argparse

from lakehouse.session import get_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("statement")
    parser.add_argument("--rows", type=int, default=50, help="maximum rows to print")
    args = parser.parse_args()

    spark = get_spark("sql")
    spark.sql(args.statement).show(args.rows, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
