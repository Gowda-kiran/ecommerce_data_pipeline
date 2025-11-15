"""
Real-Time Streaming from Kafka to Apache Iceberg
Demonstrates exactly-once semantics and ACID writes
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)
from config.spark_iceberg_config import create_iceberg_spark_session


# Define schema for incoming events
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("event_timestamp", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("device_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_value", DoubleType(), True)
])


def create_iceberg_events_table(spark):
    """Create Iceberg table for storing events."""

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS lakehouse
    """)

    # Drop table if exists (for demo purposes)
    spark.sql("DROP TABLE IF EXISTS lakehouse.events")

    # Create Iceberg table with partitioning
    spark.sql("""
        CREATE TABLE lakehouse.events (
            event_id STRING NOT NULL,
            user_id STRING NOT NULL,
            event_type STRING NOT NULL,
            product_id STRING,
            event_timestamp TIMESTAMP NOT NULL,
            session_id STRING NOT NULL,
            device_type STRING,
            page_url STRING,
            referrer STRING,
            user_agent STRING,
            ip_address STRING,
            city STRING,
            country STRING,
            event_value DOUBLE,
            ingestion_time TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (days(event_timestamp))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    print("✅ Iceberg table 'lakehouse.events' created successfully")


def stream_kafka_to_iceberg(
    spark,
    kafka_servers="localhost:9092",
    kafka_topic="user_events",
    checkpoint_location="data/checkpoints/kafka_to_iceberg"
):
    """
    Stream events from Kafka to Iceberg table.

    Args:
        spark: SparkSession
        kafka_servers: Kafka bootstrap servers
        kafka_topic: Kafka topic to consume
        checkpoint_location: Checkpoint location for exactly-once semantics
    """

    print(f"🚀 Starting Kafka → Iceberg streaming pipeline")
    print(f"📊 Kafka Servers: {kafka_servers}")
    print(f"📝 Topic: {kafka_topic}")
    print(f"💾 Checkpoint: {checkpoint_location}")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON from Kafka
    events_df = kafka_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # Add processing timestamp and convert event_timestamp
    enriched_df = events_df \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("event_timestamp",
                   to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # Data quality checks
    clean_df = enriched_df.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_timestamp").isNotNull()
    )

    # Write to Iceberg with exactly-once semantics
    query = clean_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("path", "lakehouse.events") \
        .option("checkpointLocation", checkpoint_location) \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"✅ Streaming query started (ID: {query.id})")
    print(f"📊 Monitoring at http://localhost:4040")

    return query


def stream_kafka_to_iceberg_with_aggregations(
    spark,
    kafka_servers="localhost:9092",
    kafka_topic="user_events",
    checkpoint_location="data/checkpoints/kafka_aggregations"
):
    """
    Stream from Kafka, perform aggregations, write to Iceberg.

    Demonstrates windowed aggregations with exactly-once semantics.
    """

    print(f"🚀 Starting Kafka → Aggregations → Iceberg pipeline")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse events
    events_df = kafka_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    # Convert timestamp
    events_with_ts = events_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    # Windowed aggregations (5-minute tumbling windows)
    windowed_counts = events_with_ts \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("event_type"),
            col("device_type")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("device_type"),
            col("count").alias("event_count"),
            current_timestamp().alias("processing_time")
        )

    # Write aggregations to Iceberg
    query = windowed_counts.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("path", "lakehouse.event_aggregations") \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="30 seconds") \
        .start()

    print(f"✅ Aggregation streaming query started (ID: {query.id})")

    return query


def monitor_streaming_query(query):
    """Monitor streaming query progress."""

    print("\n" + "="*60)
    print("STREAMING QUERY MONITORING")
    print("="*60)

    # Wait for query to process some data
    query.awaitTermination(timeout=60)

    # Get query status
    status = query.status
    print(f"Status: {status}")

    # Get recent progress
    progress = query.recentProgress
    if progress:
        latest = progress[-1]
        print(f"\nLatest Progress:")
        print(f"  Batch ID: {latest.get('batchId', 'N/A')}")
        print(f"  Num Input Rows: {latest.get('numInputRows', 0)}")
        print(f"  Input Rows/Sec: {latest.get('inputRowsPerSecond', 0)}")
        print(f"  Process Rows/Sec: {latest.get('processedRowsPerSecond', 0)}")


def main():
    """Main execution function."""

    # Create Spark session with Iceberg
    spark = create_iceberg_spark_session(
        app_name="Kafka to Iceberg Streaming",
        warehouse_path="data/iceberg_warehouse"
    )

    try:
        # Create Iceberg table
        create_iceberg_events_table(spark)

        # Start streaming query
        query = stream_kafka_to_iceberg(
            spark,
            kafka_servers="localhost:9092",
            kafka_topic="user_events"
        )

        # Monitor the query
        print("\n⏳ Streaming in progress... (Press Ctrl+C to stop)")
        monitor_streaming_query(query)

        # Wait for termination
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n\n🛑 Stopping streaming query...")
        query.stop()
        print("✅ Streaming query stopped")

    finally:
        # Query the Iceberg table
        print("\n📊 Querying Iceberg table...")
        result = spark.sql("""
            SELECT
                event_type,
                device_type,
                country,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM lakehouse.events
            GROUP BY event_type, device_type, country
            ORDER BY event_count DESC
        """)
        result.show(20)

        # Show table metadata
        print("\n📋 Table Snapshots:")
        spark.sql("SELECT * FROM lakehouse.events.snapshots").show(truncate=False)

        # Stop Spark
        spark.stop()


if __name__ == "__main__":
    main()
