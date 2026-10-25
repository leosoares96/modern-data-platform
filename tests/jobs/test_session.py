from lakehouse.session import CATALOG, catalog_config


def test_catalog_is_rest_and_default():
    config = catalog_config({})
    assert config[f"spark.sql.catalog.{CATALOG}.type"] == "rest"
    assert config["spark.sql.defaultCatalog"] == CATALOG


def test_defaults_point_at_the_compose_services():
    config = catalog_config({})
    assert config["spark.sql.catalog.lake.uri"] == "http://iceberg-rest:8181"
    assert config["spark.sql.catalog.lake.s3.endpoint"] == "http://minio:9000"
    assert config["spark.sql.catalog.lake.s3.path-style-access"] == "true"


def test_environment_overrides_defaults():
    env = {"ICEBERG_REST_URI": "http://localhost:8181", "AWS_REGION": "sa-east-1"}
    config = catalog_config(env)
    assert config["spark.sql.catalog.lake.uri"] == "http://localhost:8181"
    assert config["spark.sql.catalog.lake.client.region"] == "sa-east-1"


def test_timezone_is_utc():
    assert catalog_config({})["spark.sql.session.timeZone"] == "UTC"
