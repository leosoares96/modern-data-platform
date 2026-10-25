FROM apache/airflow:2.10.3-python3.10

ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.6.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Airflow submits Spark jobs in client mode, so the driver runs in this container and needs a Java
# runtime plus the same Spark distribution and jars as the cluster. Python is 3.10 here and in the
# Spark image, which PySpark requires between driver and executors.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | tar -xz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark

ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:/opt/spark/bin"

RUN mkdir -p /opt/platform/spark /opt/platform/scripts && chown -R airflow /opt/platform

USER airflow
