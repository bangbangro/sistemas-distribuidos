"""
Spark Structured Streaming job for metrics aggregation.

Reads JSON metrics from Kafka topic 'metrics-topic', applies sliding window
aggregations (1 min window, 30s slide, 2 min watermark), and writes results
to Elasticsearch index 'metrics-aggregated' and console.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, FloatType, IntegerType, TimestampType
)
from pyspark.sql.functions import (
    from_json, col, window, count, avg, sum as _sum, when, lit,
    percentile_approx, to_timestamp
)


# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.environ.get("ELASTICSEARCH_PORT", "9200")

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("MetricsStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Schema for incoming Kafka JSON messages
# ---------------------------------------------------------------------------
metrics_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("query_id", StringType(), True),
    StructField("query_type", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("cache_hit", BooleanType(), True),
    StructField("latency_ms", FloatType(), True),
    StructField("retries", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("consumer_id", StringType(), True),
    StructField("recovered", BooleanType(), True),
])

# ---------------------------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "metrics-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse the JSON value payload
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), metrics_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# ---------------------------------------------------------------------------
# Sliding window aggregation
# ---------------------------------------------------------------------------
windowed_metrics = parsed_stream \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds")) \
    .agg(
        # Throughput: count of successful events
        count(when(col("status") == "success", True)).alias("throughput"),

        # Total queries
        count("*").alias("total_queries"),

        # Latency percentiles
        percentile_approx(col("latency_ms"), 0.5).alias("latency_p50"),
        percentile_approx(col("latency_ms"), 0.95).alias("latency_p95"),

        # Average latency
        avg(col("latency_ms")).alias("avg_latency"),

        # Cache hit rate: sum(cache_hit as int) / count
        (_sum(col("cache_hit").cast("int")) / count("*")).alias("hit_rate"),

        # Retry rate: events with retries > 0 / total
        (count(when(col("retries") > 0, True)) / count("*")).alias("retry_rate"),

        # Recovery helpers (combined later to avoid division by zero)
        count(when(col("recovered") == True, True)).alias("_recovered_count"),
        count(when(col("retries") > 0, True)).alias("_retried_count"),

        # DLQ count
        count(when(col("status") == "dlq", True)).alias("dlq_count"),
    ) \
    .withColumn(
        "recovery_rate",
        when(col("_retried_count") > 0,
             col("_recovered_count") / col("_retried_count"))
        .otherwise(lit(0.0))
    ) \
    .drop("_recovered_count", "_retried_count") \
    .withColumn("window_start", col("window.start").cast("string")) \
    .withColumn("window_end", col("window.end").cast("string")) \
    .drop("window")


# ---------------------------------------------------------------------------
# Elasticsearch foreachBatch writer
# ---------------------------------------------------------------------------
es_host = ELASTICSEARCH_HOST
es_port = ELASTICSEARCH_PORT


def write_to_es(batch_df, batch_id):
    """Write a micro-batch DataFrame to Elasticsearch."""
    if batch_df.count() > 0:
        batch_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "metrics-aggregated") \
            .option("es.nodes", es_host) \
            .option("es.port", es_port) \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()


# ---------------------------------------------------------------------------
# Start streaming queries
# ---------------------------------------------------------------------------

# 1. Write to Elasticsearch via foreachBatch
es_query = windowed_metrics.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_es) \
    .option("checkpointLocation", "/tmp/checkpoint-es") \
    .start()

# 2. Write to console for debugging
console_query = windowed_metrics.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoint-console") \
    .start()

print("=== Streaming queries started ===")
print(f"  Kafka        : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"  Elasticsearch: {es_host}:{es_port}")

# Block until any query terminates
spark.streams.awaitAnyTermination()
