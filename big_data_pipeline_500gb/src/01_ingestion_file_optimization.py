"""
500GB E-commerce Big Data Pipeline - Data Ingestion & File Optimization
Author: Data Engineering Team
Purpose: Production-grade ingestion with small files optimization
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, date_format,
    year, month, dayofmonth, when, coalesce, sha2, concat_ws,
    from_json, size, count, sum as _sum, avg, min as _min, max as _max,
    row_number, rank
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, LongType
)
from pyspark.sql.window import Window
from datetime import datetime
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """
    Ingestion pipeline with file optimization for 500GB daily data.

    Handles:
    - Small files problem
    - Corrupt/malformed data
    - Optimal file sizing (512MB-1GB)
    - Predicate pushdown
    - Column pruning
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.metrics = {}

    def read_transactions(
        self,
        path: str,
        start_date: str = None,
        end_date: str = None
    ):
        """
        Read raw transaction data from Parquet with optimizations.

        Args:
            path: S3/HDFS path to transaction data
            start_date: Optional date filter for predicate pushdown
            end_date: Optional date filter for predicate pushdown

        Returns:
            DataFrame with raw transactions
        """
        logger.info(f"Reading transactions from {path}")

        # Track initial metrics
        start_time = datetime.now()

        # Read with schema inference and predicate pushdown
        df = self.spark.read \
            .format("parquet") \
            .option("mergeSchema", "false") \
            .option("compression", "snappy")

        # Apply partition filter if provided (predicate pushdown)
        if start_date:
            df = df.option("basePath", path) \
                .option("pathGlobFilter", f"*date>={start_date}*")

        df = df.load(path)

        # Column pruning - select only needed columns
        required_columns = [
            "transaction_id",
            "user_id",
            "product_id",
            "amount",
            "currency",
            "device_type",
            "status",
            "created_at"
        ]

        df = df.select(*required_columns)

        # Apply date filter if provided
        if start_date and end_date:
            df = df.filter(
                (col("created_at") >= start_date) &
                (col("created_at") <= end_date)
            )

        # Count records and partitions
        record_count = df.count()
        partition_count = df.rdd.getNumPartitions()

        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"Read {record_count:,} transactions in {duration:.2f}s")
        logger.info(f"Initial partitions: {partition_count}")

        self.metrics['read_transactions'] = {
            'record_count': record_count,
            'partition_count': partition_count,
            'duration_seconds': duration
        }

        return df

    def read_users(self, path: str):
        """
        Read user data from JSON with nested field handling.

        Args:
            path: S3/HDFS path to user data

        Returns:
            DataFrame with user data
        """
        logger.info(f"Reading users from {path}")

        # Define schema for better performance
        user_schema = StructType([
            StructField("user_id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("signup_date", TimestampType(), True),
            StructField("device_info", StringType(), True)  # JSON string
        ])

        df = self.spark.read \
            .schema(user_schema) \
            .json(path)

        # Parse nested device_info JSON
        device_schema = StructType([
            StructField("device_type", StringType()),
            StructField("os", StringType()),
            StructField("browser", StringType())
        ])

        df = df.withColumn(
            "device_info_parsed",
            from_json(col("device_info"), device_schema)
        )

        # Flatten nested structure
        df = df.select(
            "user_id",
            "name",
            "country",
            "signup_date",
            col("device_info_parsed.device_type").alias("user_device_type"),
            col("device_info_parsed.os").alias("user_os"),
            col("device_info_parsed.browser").alias("user_browser")
        )

        logger.info(f"Read {df.count():,} users")

        return df

    def read_products(self, path: str):
        """
        Read product catalog from CSV.

        Args:
            path: S3/HDFS path to product data

        Returns:
            DataFrame with product catalog
        """
        logger.info(f"Reading products from {path}")

        # Define schema
        product_schema = StructType([
            StructField("product_id", LongType(), False),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("updated_at", TimestampType(), True)
        ])

        df = self.spark.read \
            .schema(product_schema) \
            .option("header", "true") \
            .option("mode", "DROPMALFORMED") \
            .csv(path)

        logger.info(f"Read {df.count():,} products")

        return df

    def handle_corrupt_data(self, df):
        """
        Detect and handle corrupt/malformed rows.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame with corrupt row tracking
        """
        logger.info("Handling corrupt/malformed data")

        initial_count = df.count()

        # Track rows with null required fields
        corrupt_df = df.filter(
            col("transaction_id").isNull() |
            col("user_id").isNull() |
            col("product_id").isNull() |
            col("amount").isNull()
        )

        corrupt_count = corrupt_df.count()

        if corrupt_count > 0:
            logger.warning(f"Found {corrupt_count:,} corrupt rows")

            # Write corrupt data to error table for investigation
            corrupt_df.write \
                .mode("append") \
                .parquet("s3://data-lake/error/corrupt_transactions/")

        # Filter out corrupt rows
        clean_df = df.filter(
            col("transaction_id").isNotNull() &
            col("user_id").isNotNull() &
            col("product_id").isNotNull() &
            col("amount").isNotNull()
        )

        final_count = clean_df.count()

        logger.info(f"Filtered {initial_count - final_count:,} corrupt rows")
        logger.info(f"Clean records: {final_count:,}")

        self.metrics['corrupt_handling'] = {
            'initial_count': initial_count,
            'corrupt_count': corrupt_count,
            'clean_count': final_count
        }

        return clean_df

    def optimize_file_sizes(self, df, target_file_size_mb: int = 512):
        """
        Fix small files problem by repartitioning to optimal size.

        Target: 512MB - 1GB per file

        Args:
            df: Input DataFrame
            target_file_size_mb: Target file size in MB

        Returns:
            Optimized DataFrame
        """
        logger.info(f"Optimizing file sizes (target: {target_file_size_mb}MB)")

        # Get current partition count
        current_partitions = df.rdd.getNumPartitions()

        # Estimate data size (rough calculation)
        # Sample to estimate average row size
        sample_df = df.limit(10000)

        # Serialize sample to estimate size
        import json
        sample_rows = sample_df.collect()
        estimated_row_size = len(json.dumps([row.asDict() for row in sample_rows])) / len(sample_rows)

        total_rows = df.count()
        estimated_total_size_mb = (estimated_row_size * total_rows) / (1024 * 1024)

        # Calculate optimal partition count
        # Target: 512MB - 1GB per partition
        optimal_partitions = int(estimated_total_size_mb / target_file_size_mb)

        # Ensure minimum partitions for parallelism
        optimal_partitions = max(optimal_partitions, 200)

        logger.info(f"Current partitions: {current_partitions}")
        logger.info(f"Estimated data size: {estimated_total_size_mb:.2f} MB")
        logger.info(f"Optimal partitions: {optimal_partitions}")

        # Repartition if needed
        if current_partitions != optimal_partitions:
            if current_partitions > optimal_partitions:
                # Use coalesce for reducing partitions (no full shuffle)
                df = df.coalesce(optimal_partitions)
                logger.info(f"Coalesced to {optimal_partitions} partitions")
            else:
                # Use repartition for increasing partitions
                df = df.repartition(optimal_partitions)
                logger.info(f"Repartitioned to {optimal_partitions} partitions")

        self.metrics['file_optimization'] = {
            'initial_partitions': current_partitions,
            'optimal_partitions': optimal_partitions,
            'estimated_size_mb': estimated_total_size_mb,
            'target_file_size_mb': target_file_size_mb
        }

        return df

    def write_optimized(
        self,
        df,
        path: str,
        partition_by: list = None,
        mode: str = "overwrite"
    ):
        """
        Write DataFrame with optimizations.

        Args:
            df: DataFrame to write
            path: Output path
            partition_by: Partition columns
            mode: Write mode
        """
        logger.info(f"Writing optimized data to {path}")

        start_time = datetime.now()

        writer = df.write \
            .format("parquet") \
            .option("compression", "snappy") \
            .mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)
            logger.info(f"Partitioning by: {partition_by}")

        writer.save(path)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"Write completed in {duration:.2f}s")

        self.metrics['write'] = {
            'path': path,
            'partition_by': partition_by,
            'duration_seconds': duration
        }

    def get_metrics(self):
        """Return pipeline metrics."""
        return self.metrics


def main():
    """Main ingestion pipeline."""

    # Create Spark session with optimizations
    spark = SparkSession.builder \
        .appName("Ingestion-FileOptimization") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "536870912") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()

    logger.info("=" * 80)
    logger.info("STARTING DATA INGESTION & FILE OPTIMIZATION PIPELINE")
    logger.info("=" * 80)

    # Initialize pipeline
    pipeline = DataIngestionPipeline(spark)

    # Example paths (replace with actual S3/HDFS paths)
    transactions_path = "s3://data-lake/raw/transactions/"
    users_path = "s3://data-lake/raw/users/"
    products_path = "s3://data-lake/raw/products/"
    output_path = "s3://data-lake/bronze/transactions/"

    # Read data
    transactions_df = pipeline.read_transactions(
        transactions_path,
        start_date="2024-01-01",
        end_date="2024-01-31"
    )

    # Handle corrupt data
    clean_df = pipeline.handle_corrupt_data(transactions_df)

    # Optimize file sizes
    optimized_df = pipeline.optimize_file_sizes(clean_df, target_file_size_mb=512)

    # Write optimized data
    pipeline.write_optimized(
        optimized_df,
        output_path,
        partition_by=["created_at"],
        mode="overwrite"
    )

    # Print metrics
    logger.info("=" * 80)
    logger.info("PIPELINE METRICS")
    logger.info("=" * 80)

    import json
    metrics = pipeline.get_metrics()
    logger.info(json.dumps(metrics, indent=2, default=str))

    logger.info("=" * 80)
    logger.info("INGESTION PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
