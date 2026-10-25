"""Write and read an Iceberg table end to end.

Proves that Spark, the catalog and the object store agree with each other.
"""

from lakehouse.ddl import ensure_lakehouse
from lakehouse.session import get_spark

TABLE = "lake.ops.smoke"


def main() -> None:
    spark = get_spark("smoke-test")
    ensure_lakehouse(spark)

    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"CREATE TABLE {TABLE} (id bigint, note string) USING iceberg")
    spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    rows = spark.table(TABLE).count()
    assert rows == 3, f"expected 3 rows, found {rows}"

    snapshots = spark.sql(f"SELECT count(*) AS n FROM {TABLE}.snapshots").first()["n"]
    assert snapshots >= 1, "Iceberg recorded no snapshot for the insert"

    spark.sql(f"DROP TABLE {TABLE}")
    spark.stop()
    print("SMOKE OK")


if __name__ == "__main__":
    main()
