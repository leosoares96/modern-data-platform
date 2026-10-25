"""Create the namespaces and tables of the lakehouse. Safe to run repeatedly."""

from lakehouse.ddl import NAMESPACES, ensure_lakehouse
from lakehouse.session import get_spark


def main() -> None:
    spark = get_spark("init-lakehouse")
    ensure_lakehouse(spark)
    for namespace in NAMESPACES:
        for row in spark.sql(f"SHOW TABLES IN lake.{namespace}").collect():
            print(f"table: lake.{row['namespace']}.{row['tableName']}")
    spark.stop()


if __name__ == "__main__":
    main()
