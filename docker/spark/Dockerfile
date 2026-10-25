FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.6.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Iceberg (Spark runtime + S3 support) and the Kafka source. Baked into the image so jobs never
# resolve dependencies at submit time and work without internet access at run time.
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar /opt/spark/jars/

# Code and scripts are bind-mounted by Compose. The checkpoints directory is created here, owned by
# the spark user, so a named volume mounted on it inherits writable permissions.
RUN mkdir -p /opt/platform/spark /opt/platform/scripts /opt/platform/checkpoints \
    && chown -R spark:spark /opt/platform

USER spark
WORKDIR /opt/platform
