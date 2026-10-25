# Spark 3.5.1 + thư viện cho driver và executor:
#   - pyvi: tách từ tiếng Việt; pandas/pyarrow: pyspark.ml.functions cần khi import
#   - Delta Lake 3.2.0 (time-travel, OPTIMIZE, VACUUM)
#   - Kafka connector cho Spark Structured Streaming
FROM bitnamilegacy/spark:3.5.1

USER root
RUN pip install --no-cache-dir pyvi==0.1.1 "pandas<2.3" "pyarrow<18" \
 && pip install --no-cache-dir --no-deps delta-spark==3.2.0

ARG MAVEN=https://repo1.maven.org/maven2
ARG JARS=/opt/bitnami/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar ${JARS}
ADD --chmod=644 ${MAVEN}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar ${JARS}
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar ${JARS}
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar ${JARS}
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar ${JARS}
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar ${JARS}
USER 1001
