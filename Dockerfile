FROM apache/spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

# Download runtime dependency JARs into Spark's jars directory
# These are marked "provided" in build.sbt — baked into image, not the fat JAR
RUN cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.1/iceberg-spark-runtime-3.4_2.12-1.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

# JMX Prometheus exporter for monitoring
RUN mkdir -p /app && \
    wget -q https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar \
    -O /app/jmx_prometheus_javaagent-0.11.0.jar

USER spark
