FROM bitnami/spark:3.5

USER root

RUN pip install pyspark==3.5.0

COPY spark_streaming.py /app/spark_streaming.py

ENV PYTHONUNBUFFERED=1

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0", \
     "/app/spark_streaming.py"]
