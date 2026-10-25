# 0006. At-least-once bronze, idempotent silver

- Status: Accepted
- Date: 2026-10-25

## Context

The bronze job reads Kafka with Structured Streaming and writes to Iceberg from `foreachBatch`. Spark
checkpoints the offsets it has processed, but the Iceberg commit and the checkpoint update are two
separate steps. If the job dies in between, the micro-batch is replayed and its rows are appended again.
Exactly-once would need an idempotent writer that tracks batch ids, which is more machinery than the
benefit justifies here. The producer can also publish the same event twice on purpose.

## Decision

Accept duplicates in bronze and make silver the place where they disappear:

- every event has a unique `event_id`;
- silver keeps one row per `event_id` using `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`;
- the incremental read overlaps the previous run by two minutes, so a bronze commit that lands late is
  never skipped, and the overlap is harmless because the merge is idempotent.

## Consequences

- Bronze row counts can exceed the number of distinct events. That is expected and tested.
- The demo runs silver twice and requires the second run to insert nothing.
- Anything that reads bronze directly must deduplicate on its own.
