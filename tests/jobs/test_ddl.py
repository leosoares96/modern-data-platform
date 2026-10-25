import re

from lakehouse.ddl import NAMESPACES, TABLES


def test_every_table_lives_in_a_known_namespace():
    for name in TABLES:
        assert name.split(".")[0] in NAMESPACES


def test_statements_are_idempotent_iceberg_tables():
    for name, ddl in TABLES.items():
        assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS lake.{name} (")
        assert "USING iceberg" in ddl
        assert "'format-version' = '2'" in ddl


def test_silver_uses_merge_on_read():
    ddl = TABLES["silver.flights"]
    for mode in ("write.merge.mode", "write.update.mode", "write.delete.mode"):
        assert f"'{mode}' = 'merge-on-read'" in ddl


def test_event_tables_are_partitioned_by_day():
    for name in ("bronze.flights_raw", "silver.flights"):
        assert "PARTITIONED BY (days(event_ts))" in TABLES[name]


def test_silver_keeps_the_dedup_key_and_processing_columns():
    columns = set(re.findall(r"^\s+(\w+) ", TABLES["silver.flights"], flags=re.MULTILINE))
    assert {"event_id", "event_ts", "processed_at", "is_late"} <= columns


def test_every_layer_is_covered():
    layers = {name.split(".")[0] for name in TABLES}
    assert layers == set(NAMESPACES)
