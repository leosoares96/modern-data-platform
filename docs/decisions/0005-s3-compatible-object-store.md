# 0005. S3-compatible object store: a MinIO fork

- Status: Accepted
- Date: 2026-10-25

## Context

The warehouse lives on an S3-compatible store so the same code works on AWS. The project was designed
around MinIO, but the `minio/minio` and `minio/mc` repositories are gone from Docker Hub: upstream no
longer publishes community container images. Pinning a tag that cannot be pulled would make the project
impossible to build.

Candidates checked on 2026-10-25: `pgsty/minio` (maintained fork, same CLI and variables, tagged
releases), SeaweedFS, Garage, RustFS and LocalStack.

## Decision

Use `pgsty/minio` pinned to `RELEASE.2026-08-04T00-00-00Z`. It is a drop-in replacement, so nothing else
in the stack changes. Buckets are created with the official `amazon/aws-cli` image, which removes the
dependency on the `mc` client.

## Consequences

- A community fork carries more supply-chain risk than an upstream vendor. Acceptable for a local study
  environment, not for production.
- Swapping to SeaweedFS or Garage later only touches the `minio` service and the endpoint variable.
