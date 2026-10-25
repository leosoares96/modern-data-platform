# 0007. Merge-on-read for silver

- Status: Accepted
- Date: 2026-10-25

## Context

Iceberg can apply row-level changes in two ways. Copy-on-write rewrites every data file touched by a
`MERGE`, which keeps reads fast but makes each write expensive. Merge-on-read writes small delete files
and applies them when the table is read, which makes writes cheap and reads a bit slower until the files
are compacted.

## Decision

Configure silver with merge-on-read for merge, update and delete. Silver is written every few minutes
with a small number of new rows, which is the case where rewriting whole files is wasteful.

## Consequences

- Silver needs regular compaction. The maintenance job runs `rewrite_data_files` for this reason.
- Today the merge only inserts, so no delete files are produced yet. The setting is there so a future
  correction flow (updating or deleting events) does not require a table migration.
- Gold is rebuilt per hour and keeps the default copy-on-write behaviour.
