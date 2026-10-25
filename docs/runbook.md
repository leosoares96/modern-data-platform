# Runbook

How to run and operate the platform locally. For the design, see [architecture.md](architecture.md).

## Memory and profiles

The whole stack needs roughly 8 to 10 GB of memory for Docker. On Windows, Docker Desktop uses WSL2, which
takes half of the host memory by default. Raise it in `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=10GB
processors=4
```

If that is too much, start only what you need (see [ADR 0002](decisions/0002-compose-profiles.md)):

| You want to... | Command | Needs about |
| --- | --- | --- |
| Produce and look at Kafka events | `docker compose --profile ingest up -d --build` | 1.5 GB |
| Explore the lakehouse and run Spark jobs | `docker compose --profile lake up -d --build` | 5 GB |
| Run the continuous bronze ingestion | `docker compose --profile stream up -d --build` | 7 GB |
| Run Airflow on top of the lakehouse | `docker compose --profile orchestration up -d --build` | 7 GB |
| Everything | `make up` | 9 to 10 GB |

Shrink the Spark worker with `SPARK_WORKER_MEMORY` and `SPARK_WORKER_CORES` in `.env` (copy
`.env.example`). `make` is not installed by default on Windows: run the commands above from WSL, or use the
raw `docker compose` commands.

## Run the pipeline end to end

```bash
scripts/demo.sh
```

It starts from a clean state, produces 400 events with injected faults, drains Kafka into bronze,
builds silver, gold and the Iceberg showcase, runs maintenance and finally runs the Airflow tasks, asserting
the result of every stage. This is the same script CI runs.

## Where to look

| What | Where |
| --- | --- |
| Spark master and running applications | http://localhost:8080 |
| Spark worker | http://localhost:8081 |
| Object store console | http://localhost:9001 (`minioadmin` / `minioadmin`) |
| Iceberg REST catalog | http://localhost:8181/v1/config |
| Airflow | http://localhost:8088 (`admin` / `admin`) |
| Kafka from the host | `localhost:9092` |

## Query the tables

```bash
make sql Q="SELECT airline, count(*) AS rows FROM lake.silver.flights GROUP BY airline"
make sql Q="SELECT reason, count(*) FROM lake.bronze.flights_dead_letter GROUP BY reason"
make sql Q="SELECT * FROM lake.silver.flights.snapshots"
make sql Q="SELECT check_name, passed, observed FROM lake.quality.check_results ORDER BY checked_at DESC"
```

Without `make`:

```bash
docker compose --profile all exec -T spark-client bash /opt/platform/scripts/submit.sh \
  /opt/platform/spark/admin/sql.py "SELECT count(*) FROM lake.silver.flights"
```

## Operate the jobs

| Task | Command |
| --- | --- |
| Run silver by hand | `docker compose --profile all exec -T spark-client bash /opt/platform/scripts/submit.sh /opt/platform/spark/batch/silver_job.py` |
| Rebuild silver from all of bronze | the same, with `--full-refresh` |
| Drain Kafka once and stop | `.../spark/streaming/adsb_stream.py --once` |
| Compact and expire snapshots | `.../spark/batch/maintenance_job.py` |

Silver and gold are idempotent, so running them again is always safe.

### Reprocess from Kafka

The streaming job keeps its progress in a checkpoint on the `spark-checkpoints` volume. To read the topic
from the beginning again, stop the stream, delete the checkpoint and start it again. Bronze is append only,
so this duplicates rows there; silver removes them on the next run.

```bash
docker compose --profile all stop bronze-stream
docker compose --profile all run --rm --entrypoint rm bronze-stream -rf /opt/platform/checkpoints/bronze_flights
docker compose --profile all up -d bronze-stream
```

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `Initial job has not accepted any resources` | The worker is not registered yet or has no memory left. Check http://localhost:8080 and lower `SPARK_EXECUTOR_MEMORY`. |
| Containers are killed with exit code 137 | Docker ran out of memory. Raise the WSL2 limit or start fewer profiles. |
| Jobs fail with connection errors to `iceberg-rest` | The catalog has no healthcheck. Wait for `curl localhost:8181/v1/config` to answer. |
| `bad interpreter: /bin/bash^M` | A script was checked out with CRLF line endings. `.gitattributes` prevents this; run `git add --renormalize .`. |
| The streaming job keeps restarting | It restarts until the catalog and the cluster are up. Read its log with `docker compose logs bronze-stream`. |

## Reset

```bash
scripts/reset_environment.sh        # or: make reset
```

This deletes every volume: Kafka offsets, the object store and the Airflow database.
