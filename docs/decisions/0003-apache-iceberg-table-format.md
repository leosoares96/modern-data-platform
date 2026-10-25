# 0003. Apache Iceberg as the table format

- Status: Accepted
- Date: 2026-10-25

## Context

The lakehouse needs ACID tables on object storage with upserts, schema evolution and the ability to
query past states for auditing. Delta Lake, Apache Hudi and Apache Iceberg all provide this.

## Decision

Use Apache Iceberg (format version 2).

- It is engine-neutral: Spark writes here, Trino can read the same tables later, and AWS Glue, Athena and
  EMR all support it, which is the path this project is meant to prepare for.
- Hidden partitioning (`days(event_ts)`) keeps partition columns out of queries and allows partition
  evolution without rewriting data.
- Table maintenance is exposed as Spark procedures (`rewrite_data_files`, `expire_snapshots`).

## Consequences

- Row-level changes need format version 2 and the Iceberg Spark extensions.
- Streaming writes create many small files, so a maintenance job is a requirement, not an option.
