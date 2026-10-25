# 0002. Split the stack into Compose profiles

- Status: Accepted
- Date: 2026-10-25

## Context

The full stack (Kafka, an object store, an Iceberg catalog, a Spark master and worker, Spark drivers,
Postgres and Airflow) needs roughly 8 to 10 GB of memory. The author's machine cannot run all of it at
once, and neither can most laptops running Docker through WSL2, which takes half of the host memory by
default.

## Decision

Group services into profiles and give each service every profile that needs it:

| Profile | Starts |
| --- | --- |
| `ingest` | Kafka, topic creation, event producer |
| `lake` | object store, Iceberg REST catalog, Spark master, worker and client |
| `stream` | `ingest` + `lake` + the bronze streaming job |
| `orchestration` | `lake` + Postgres + Airflow |
| `all` | everything |

Cross-profile `depends_on` entries are avoided because Compose rejects a dependency that lives in an
inactive profile. Services retry their connections instead.

## Consequences

- A phase can be developed and demonstrated with only the services it needs.
- Each service lists several profiles, which is more verbose than a single flat file.
- `make up` still starts everything for anyone with enough memory, and CI does exactly that.
