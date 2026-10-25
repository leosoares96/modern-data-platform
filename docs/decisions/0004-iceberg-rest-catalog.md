# 0004. Iceberg REST catalog

- Status: Accepted
- Date: 2026-10-25

## Context

Iceberg needs a catalog to map table names to metadata locations. Options are the Hadoop catalog (no
service, but unsafe with concurrent writers on object storage), the JDBC catalog (needs a database and
its driver on every client) and the REST catalog (an HTTP service defined by an open specification).

## Decision

Use the REST catalog, served by the `tabulario/iceberg-rest` image. Spark only needs a URI, and the
server owns the metadata writes.

## Consequences

- Moving to AWS later means changing the catalog configuration in `lakehouse/session.py`, not the jobs.
- One more container to run. The image has no healthcheck, so scripts wait for `/v1/config`.
- Credentials are static development values; a production setup would use vended credentials.
