# =============================================================================
# Dockerfile - Spark 3.5.3 + Iceberg 1.5.0 + GCS
# =============================================================================

FROM apache/spark:3.5.3

USER root

ENV SPARK_HOME=/opt/spark
ENV PYTHONUNBUFFERED=1

# -----------------------------------------------------------------------------
# Python Dependencies
# -----------------------------------------------------------------------------
RUN pip install --no-cache-dir \
    requests \
    pytest

# -----------------------------------------------------------------------------
# Install GCS Connector (shaded)
# -----------------------------------------------------------------------------
RUN curl -sL \
    -o /opt/spark/jars/gcs-connector-hadoop3-2.2.22-shaded.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar

# -----------------------------------------------------------------------------
# Intall Iceberg runtime for Spark 3.5 (Scala 2.12)
# -----------------------------------------------------------------------------
RUN curl -sL \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar


WORKDIR /opt/spark/work-dir

CMD ["tail", "-f", "/dev/null"]
