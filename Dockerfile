FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

RUN curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

RUN curl -L -o /opt/spark/jars/clickhouse-jdbc-0.6.4-shaded.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.4/clickhouse-jdbc-0.6.4-shaded.jar

RUN curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

COPY target/scala-2.12/*-assembly*.jar /opt/spark/jars/app.jar