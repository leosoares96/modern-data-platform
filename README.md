# Modern Data Platform

> **⚠️ Status: early scaffold.** The directory layout is in place and a Kafka broker runs,
> but the Spark jobs, Airflow DAGs, scripts and docs are still empty placeholders.
> See [Current state](#current-state) before cloning. Study project.

A local lakehouse study environment, designed to run entirely on Docker. The goal is a
medallion pipeline (bronze → silver → gold) over **Apache Iceberg**, fed by a Kafka stream
and orchestrated with Airflow — no cloud account required.

## Current state

| Component | Status |
| --- | --- |
| Kafka broker (KRaft, single node) | ✅ runs with `docker compose up` |
| Synthetic flight-event generator | ✅ prints JSON events to stdout |
| Spark batch jobs (silver / gold) | ⬜ empty files |
| Spark streaming job | ⬜ empty file |
| Airflow DAGs (silver / gold) | ⬜ empty files |
| MinIO (S3-compatible storage) | ⬜ not in compose yet |
| Apache Iceberg tables | ⬜ not implemented yet |
| Bootstrap / topic scripts, Makefile | ⬜ empty files |
| Architecture docs and ADRs | ⬜ empty files |

## Intended architecture

```
 event generator ──▶ Kafka topic ──▶ Spark Structured Streaming
                                              │
                                              ▼
                                     Iceberg  bronze
                                              │  Spark batch
                                              ▼
                                     Iceberg  silver  ──▶  gold
                                              ▲
                                        Airflow DAGs
                      storage: MinIO (S3-compatible), all local
```

## Running what exists today

```bash
docker compose up -d kafka
```

```bash
docker build -t flight-producer ./producer/adsb && docker run --rm flight-producer
```

The producer generates random flight events (airline, altitude, speed, timestamp) and
writes them to stdout. Publishing them to a Kafka topic is the next step.

## Roadmap

1. Publish producer events to a Kafka topic
2. Add MinIO to `docker-compose.yml` as the object store
3. Spark Structured Streaming job: Kafka → Iceberg bronze
4. Spark batch jobs: bronze → silver → gold
5. Airflow DAGs to orchestrate the batch layer
6. Fill `docs/architecture.md` and record the first ADRs

## License

[MIT](LICENSE)
