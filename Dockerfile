FROM docker.io/library/spark:3.4.1
USER root
RUN mkdir -p /app
RUN wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar -O /app/jmx_prometheus_javaagent-0.11.0.jar
COPY target/scala-2.12/lakehouse-ingestion.jar /app/lakehouse-ingestion.jar
COPY src/main/resources/ /app/resources/
USER spark