#!/usr/bin/env bash
# Initialise the metadata database, create a local admin user and run the scheduler and the web
# server in one container. Local development only: use separate containers anywhere else.
set -euo pipefail

airflow db migrate

airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Local --lastname Admin \
  --role Admin --email admin@example.com || true

airflow scheduler &
exec airflow webserver --port 8080
