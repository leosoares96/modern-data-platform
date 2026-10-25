"""Spark session factory. The Iceberg REST catalog is configured from environment variables."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

CATALOG = "lake"


def catalog_config(env: dict[str, str] | None = None) -> dict[str, str]:
    """Spark settings that register the Iceberg REST catalog as `lake` and make it the default."""
    env = dict(os.environ) if env is None else env
    prefix = f"spark.sql.catalog.{CATALOG}"
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": env.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
        f"{prefix}.warehouse": env.get("WAREHOUSE", "s3://warehouse/"),
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": env.get("S3_ENDPOINT", "http://minio:9000"),
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": env.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        f"{prefix}.s3.secret-access-key": env.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        f"{prefix}.client.region": env.get("AWS_REGION", "us-east-1"),
        "spark.sql.defaultCatalog": CATALOG,
        "spark.sql.session.timeZone": "UTC",
    }


def get_spark(app_name: str, extra: dict[str, str] | None = None) -> SparkSession:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in {**catalog_config(), **(extra or {})}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
