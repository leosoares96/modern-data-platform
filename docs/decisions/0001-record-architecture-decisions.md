# 0001. Record architecture decisions

- Status: Accepted
- Date: 2026-10-25

## Context

This is a learning project that will be read by people who were not there when choices were made. The
first version of the repository described a Spark, Airflow, MinIO and Iceberg platform while most files
were empty. The reasoning behind a stack is as important as the stack itself, and it is the part that
gets lost.

## Decision

Keep a short ADR for every decision that is expensive to reverse or that a reader would reasonably
question, using the template in [README.md](README.md).

## Consequences

Slightly more writing per change. In exchange, "why X and not Y" has a stable answer.
