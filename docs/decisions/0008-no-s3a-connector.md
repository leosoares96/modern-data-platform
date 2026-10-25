# 0008. No S3A connector: local checkpoints and no orphan file removal

- Status: Accepted
- Date: 2026-10-25

## Context

Iceberg reads and writes data through its own `S3FileIO`, which needs only the `iceberg-aws-bundle` jar.
Two things in this project still go through Hadoop's FileSystem API instead, and that needs the S3A
connector (`hadoop-aws` plus the AWS SDK v1 bundle, several hundred megabytes):

- Spark Structured Streaming checkpoints;
- Iceberg's `remove_orphan_files` procedure, which lists a table's directory. The first CI run of the
  maintenance job failed with `No FileSystem for scheme "s3"`.

## Decision

Do not add the connector.

- Checkpoints live on a local Docker volume (`spark-checkpoints`).
- The maintenance job runs `rewrite_data_files` and `expire_snapshots` and skips `remove_orphan_files`.

## Consequences

- Checkpoints survive container restarts but not `make reset`, and are not shared between nodes. Fine for a
  single-node study environment, wrong for a cluster.
- Orphan files (left behind by a write that failed before commit) are never reclaimed here. Locally that
  costs a few megabytes. On AWS, run the procedure from EMR or Glue, where the S3A connector is present.
- If the connector is added later, both limitations can be lifted without changing the table layout.
