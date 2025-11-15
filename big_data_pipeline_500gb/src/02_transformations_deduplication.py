"""
500GB E-commerce Big Data Pipeline - Transformations & Deduplication
Author: Data Engineering Team
Purpose: Data cleaning, deduplication, and standardization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, upper, lower, regexp_replace,
    to_timestamp, date_format, year, month, dayofmonth,
    row_number, rank, dense_rank, md5, sha2, concat_ws,
    udf, expr, round as spark_round
)
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformationPipeline:
    """
    Transformation pipeline with cleaning, deduplication, and standardization.

    Handles:
    - Null handling
    - Data inconsistencies
    - Deduplication
    - Currency normalization
    - Data type conversions
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.metrics = {}

        # Currency conversion rates (in production, fetch from API or database)
        self.exchange_rates = {
            'USD': 1.0,
            'EUR': 1.08,
            'GBP': 1.27,
            'JPY': 0.0067,
            'INR': 0.012,
            'CNY': 0.14,
            'AUD': 0.65,
            'CAD': 0.74
        }

    def clean_nulls(self, df):
        """
        Handle null and inconsistent values.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        logger.info("Cleaning null and inconsistent values")

        initial_count = df.count()

        # Track nulls before cleaning
        null_counts = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                null_counts[column] = null_count

        logger.info(f"Null counts before cleaning: {null_counts}")

        # Clean specific columns
        df_clean = df \
            .withColumn(
                "amount",
                when(col("amount").isNull(), 0.0)
                .when(col("amount") < 0, 0.0)
                .otherwise(col("amount"))
            ) \
            .withColumn(
                "currency",
                coalesce(upper(trim(col("currency"))), lit("USD"))
            ) \
            .withColumn(
                "device_type",
                coalesce(
                    when(upper(trim(col("device_type"))).isin("MOBILE", "TABLET", "DESKTOP"),
                         upper(trim(col("device_type"))))
                    .otherwise(lit("UNKNOWN")),
                    lit("UNKNOWN")
                )
            ) \
            .withColumn(
                "status",
                coalesce(
                    when(upper(trim(col("status"))).isin("COMPLETED", "PENDING", "FAILED"),
                         upper(trim(col("status"))))
                    .otherwise(lit("UNKNOWN")),
                    lit("UNKNOWN")
                )
            )

        # Ensure timestamp is valid
        df_clean = df_clean.withColumn(
            "created_at",
            coalesce(
                to_timestamp(col("created_at")),
                lit(datetime.now())
            )
        )

        final_count = df_clean.count()

        logger.info(f"Records after cleaning: {final_count:,}")

        self.metrics['null_cleaning'] = {
            'initial_count': initial_count,
            'null_counts_before': null_counts,
            'final_count': final_count
        }

        return df_clean

    def deduplicate_transactions(self, df):
        """
        Deduplicate data based on transaction_id.

        Strategy:
        - Keep latest record based on created_at
        - Use window function for efficient deduplication

        Args:
            df: Input DataFrame

        Returns:
            Deduplicated DataFrame
        """
        logger.info("Deduplicating transactions")

        initial_count = df.count()

        # Add row number partitioned by transaction_id, ordered by created_at DESC
        window_spec = Window.partitionBy("transaction_id").orderBy(col("created_at").desc())

        df_with_rank = df.withColumn("row_num", row_number().over(window_spec))

        # Keep only the first row (most recent) for each transaction_id
        df_dedup = df_with_rank.filter(col("row_num") == 1).drop("row_num")

        final_count = df_dedup.count()
        duplicates_removed = initial_count - final_count

        logger.info(f"Removed {duplicates_removed:,} duplicates")
        logger.info(f"Final record count: {final_count:,}")

        self.metrics['deduplication'] = {
            'initial_count': initial_count,
            'duplicates_removed': duplicates_removed,
            'final_count': final_count,
            'duplicate_percentage': round(duplicates_removed / initial_count * 100, 2) if initial_count > 0 else 0
        }

        return df_dedup

    def normalize_currency(self, df):
        """
        Convert all currencies to USD standard.

        Args:
            df: Input DataFrame with currency column

        Returns:
            DataFrame with normalized amount_usd column
        """
        logger.info("Normalizing currency to USD")

        # Create UDF for currency conversion
        @udf(returnType=DoubleType())
        def convert_to_usd(amount, currency):
            if amount is None or currency is None:
                return 0.0

            rate = self.exchange_rates.get(currency, 1.0)
            return round(amount * rate, 2)

        # Apply currency conversion
        df_normalized = df.withColumn(
            "amount_usd",
            convert_to_usd(col("amount"), col("currency"))
        )

        # Add original amount as backup
        df_normalized = df_normalized.withColumn(
            "amount_original",
            col("amount")
        ).withColumn(
            "currency_original",
            col("currency")
        )

        # Log currency distribution
        currency_dist = df_normalized.groupBy("currency").count().collect()
        logger.info("Currency distribution:")
        for row in currency_dist:
            logger.info(f"  {row['currency']}: {row['count']:,}")

        self.metrics['currency_normalization'] = {
            'currencies_found': [row['currency'] for row in currency_dist],
            'exchange_rates_used': self.exchange_rates
        }

        return df_normalized

    def add_derived_columns(self, df):
        """
        Add derived columns for analytics.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with derived columns
        """
        logger.info("Adding derived columns")

        df_derived = df \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at"))) \
            .withColumn("date", date_format(col("created_at"), "yyyy-MM-dd")) \
            .withColumn("hour", date_format(col("created_at"), "HH")) \
            .withColumn(
                "amount_bucket",
                when(col("amount_usd") < 10, "0-10")
                .when(col("amount_usd") < 50, "10-50")
                .when(col("amount_usd") < 100, "50-100")
                .when(col("amount_usd") < 500, "100-500")
                .otherwise("500+")
            ) \
            .withColumn(
                "is_high_value",
                when(col("amount_usd") >= 500, True).otherwise(False)
            ) \
            .withColumn(
                "transaction_hash",
                sha2(concat_ws("-",
                    col("transaction_id"),
                    col("user_id"),
                    col("product_id")
                ), 256)
            )

        logger.info("Derived columns added successfully")

        return df_derived

    def validate_data_quality(self, df):
        """
        Run data quality checks on transformed data.

        Args:
            df: Transformed DataFrame

        Returns:
            Quality metrics dictionary
        """
        logger.info("Running data quality validation")

        total_count = df.count()

        quality_metrics = {
            'total_records': total_count,
            'checks': {}
        }

        # Check 1: No null transaction_ids
        null_tx_ids = df.filter(col("transaction_id").isNull()).count()
        quality_metrics['checks']['null_transaction_ids'] = {
            'count': null_tx_ids,
            'percentage': round(null_tx_ids / total_count * 100, 4) if total_count > 0 else 0,
            'status': 'PASS' if null_tx_ids == 0 else 'FAIL'
        }

        # Check 2: Valid amount_usd (> 0)
        invalid_amounts = df.filter(col("amount_usd") <= 0).count()
        quality_metrics['checks']['invalid_amounts'] = {
            'count': invalid_amounts,
            'percentage': round(invalid_amounts / total_count * 100, 4) if total_count > 0 else 0,
            'status': 'WARNING' if invalid_amounts > 0 else 'PASS'
        }

        # Check 3: Valid status values
        invalid_status = df.filter(
            ~col("status").isin("COMPLETED", "PENDING", "FAILED", "UNKNOWN")
        ).count()
        quality_metrics['checks']['invalid_status'] = {
            'count': invalid_status,
            'percentage': round(invalid_status / total_count * 100, 4) if total_count > 0 else 0,
            'status': 'PASS' if invalid_status == 0 else 'FAIL'
        }

        # Check 4: Valid device types
        invalid_devices = df.filter(
            ~col("device_type").isin("MOBILE", "TABLET", "DESKTOP", "UNKNOWN")
        ).count()
        quality_metrics['checks']['invalid_device_types'] = {
            'count': invalid_devices,
            'percentage': round(invalid_devices / total_count * 100, 4) if total_count > 0 else 0,
            'status': 'PASS' if invalid_devices == 0 else 'FAIL'
        }

        # Check 5: Future dates (data quality issue)
        future_dates = df.filter(col("created_at") > lit(datetime.now())).count()
        quality_metrics['checks']['future_dates'] = {
            'count': future_dates,
            'percentage': round(future_dates / total_count * 100, 4) if total_count > 0 else 0,
            'status': 'WARNING' if future_dates > 0 else 'PASS'
        }

        # Overall status
        failed_checks = sum(1 for check in quality_metrics['checks'].values() if check['status'] == 'FAIL')
        warning_checks = sum(1 for check in quality_metrics['checks'].values() if check['status'] == 'WARNING')

        quality_metrics['overall_status'] = 'PASS' if failed_checks == 0 else 'FAIL'
        quality_metrics['failed_checks'] = failed_checks
        quality_metrics['warning_checks'] = warning_checks

        logger.info(f"Quality validation complete: {quality_metrics['overall_status']}")
        logger.info(f"Failed checks: {failed_checks}, Warnings: {warning_checks}")

        self.metrics['data_quality'] = quality_metrics

        return quality_metrics

    def get_metrics(self):
        """Return pipeline metrics."""
        return self.metrics


def main():
    """Main transformation pipeline."""

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Transformations-Deduplication") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()

    logger.info("=" * 80)
    logger.info("STARTING DATA TRANSFORMATION & DEDUPLICATION PIPELINE")
    logger.info("=" * 80)

    # Initialize pipeline
    pipeline = DataTransformationPipeline(spark)

    # Read bronze data (output from ingestion)
    bronze_path = "s3://data-lake/bronze/transactions/"
    logger.info(f"Reading data from {bronze_path}")

    df = spark.read.parquet(bronze_path)

    logger.info(f"Loaded {df.count():,} records")

    # Step 1: Clean nulls and inconsistencies
    df_clean = pipeline.clean_nulls(df)

    # Step 2: Deduplicate
    df_dedup = pipeline.deduplicate_transactions(df_clean)

    # Step 3: Normalize currency
    df_normalized = pipeline.normalize_currency(df_dedup)

    # Step 4: Add derived columns
    df_final = pipeline.add_derived_columns(df_normalized)

    # Step 5: Validate data quality
    quality_metrics = pipeline.validate_data_quality(df_final)

    # Write to silver layer
    silver_path = "s3://data-lake/silver/transactions/"
    logger.info(f"Writing transformed data to {silver_path}")

    df_final.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .save(silver_path)

    # Print metrics
    logger.info("=" * 80)
    logger.info("TRANSFORMATION METRICS")
    logger.info("=" * 80)

    import json
    metrics = pipeline.get_metrics()
    logger.info(json.dumps(metrics, indent=2, default=str))

    logger.info("=" * 80)
    logger.info("TRANSFORMATION PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
