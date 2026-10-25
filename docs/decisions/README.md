# Architecture Decision Records

Each file records one decision: the context, what was decided and what it costs. Decisions are
append-only. When one is replaced, a new ADR supersedes it instead of the old one being edited.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-compose-profiles.md) | Split the stack into Compose profiles | Accepted |
| [0003](0003-apache-iceberg-table-format.md) | Apache Iceberg as the table format | Accepted |
| [0004](0004-iceberg-rest-catalog.md) | Iceberg REST catalog | Accepted |
| [0005](0005-s3-compatible-object-store.md) | S3-compatible object store: a MinIO fork | Accepted |

## Template

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by NNNN
- Date: YYYY-MM-DD

## Context
What forces are at play?

## Decision
What we are doing.

## Consequences
What gets easier, what gets harder.
```
