# Roadmap

The platform is built in small phases. Every phase ends in something that runs with one command
and is covered by the CI integration workflow (`.github/workflows/integration.yml`).

| Phase | Goal | Status |
| --- | --- | --- |
| 0 | Foundations: pinned images, healthchecks, Compose profiles, Makefile, CI, ADRs | done |
| 1 | Flight events published to Kafka, with a versioned schema and fault injection | done |
| 2 | Lakehouse storage: S3-compatible store, Iceberg REST catalog, Spark cluster | done |
| 3 | Bronze layer: Spark Structured Streaming from Kafka into Iceberg, with a dead-letter table | done |
| 4 | Silver layer: deduplication with `MERGE INTO`, cleaning rules, data quality checks | done |
| 5 | Gold layer and Iceberg showcase: aggregates, time travel, maintenance | done |
| 6 | Orchestration with Airflow: silver, gold and maintenance DAGs | done |
| 7 | Serving and observability (Trino, dashboards). Optional | - |

## Design constraints

- **Runs on a laptop.** Compose profiles (`ingest`, `lake`, `stream`, `orchestration`, `all`) let you start only
  what a phase needs. See [ADR-0002](decisions/0002-compose-profiles.md).
- **No cloud account required.** Everything is local. A port to AWS (Glue catalog, S3, Terraform) belongs in a
  separate repository and reuses these jobs unchanged because the catalog is behind the Iceberg REST spec.
- **Every claim is checked.** The README status table distinguishes what is implemented from what is verified
  in CI, and is updated in the same commit as the feature.

## Out of scope

Exactly-once delivery end to end, multi-node Kafka or Spark, security hardening, and real ADS-B ingestion.
The event schema mirrors OpenSky state vectors so a real source can be plugged in later.
