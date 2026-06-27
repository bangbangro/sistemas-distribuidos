# spark/Dockerfile
# Usamos la imagen oficial de Apache Spark 3.5.2 (basada en OpenJDK 11)
FROM apache/spark:3.5.2

USER root

# Instalar wget y descargar los conectores Kafka y Elasticsearch
RUN apt-get update && apt-get install -y wget && \
    wget -O $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.2.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.2/spark-sql-kafka-0-10_2.12-3.5.2.jar && \
    wget -O $SPARK_HOME/jars/elasticsearch-spark-30_2.12-8.12.0.jar \
        https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.12.0/elasticsearch-spark-30_2.12-8.12.0.jar && \
    chmod 644 $SPARK_HOME/jars/*.jar && apt-get clean && rm -rf /var/lib/apt/lists/*

# El contenedor se ejecutará con el comando definido en docker‑compose
