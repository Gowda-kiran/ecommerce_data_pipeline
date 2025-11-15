"""
500GB E-commerce Big Data Pipeline - Partitioning & Skew Handling
Author: Data Engineering Team
Purpose: Optimize partitioning and handle data skew for 500GB scale
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, expr, count, sum as _sum, avg,
    rand, concat, substring, md5, hash as spark_hash,
    broadcast, monotonically_increasing_id
)
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime
import logging
import math


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PartitioningSkewHandler:
    """
    Advanced partitioning and skew handling for big data processing.

    Techniques:
    - Intelligent partitioning strategy
    - Skew detection
    - Salting for skewed keys
    - Adaptive Query Execution (AQE)
    - Broadcast optimization
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.metrics = {}

    def analyze_data_distribution(self, df, partition_col: str):
        """
        Analyze data distribution to detect skew.

        Args:
            df: Input DataFrame
            partition_col: Column to analyze

        Returns:
            Distribution statistics
        """
        logger.info(f"Analyzing distribution for column: {partition_col}")

        # Count records per partition key
        dist_df = df.groupBy(partition_col) \
            .agg(count("*").alias("record_count")) \
            .orderBy(col("record_count").desc())

        # Collect statistics
        total_records = df.count()
        total_partitions = dist_df.count()

        # Get top partitions
        top_10 = dist_df.limit(10).collect()

        # Calculate skew metrics
        max_partition_size = top_10[0]['record_count'] if top_10 else 0
        avg_partition_size = total_records / total_partitions if total_partitions > 0 else 0

        # Skew ratio: max partition / avg partition
        skew_ratio = max_partition_size / avg_partition_size if avg_partition_size > 0 else 0

        distribution_stats = {
            'column': partition_col,
            'total_records': total_records,
            'total_partitions': total_partitions,
            'avg_partition_size': avg_partition_size,
            'max_partition_size': max_partition_size,
            'skew_ratio': skew_ratio,
            'is_skewed': skew_ratio > 2.0,  # Threshold: 2x average
            'top_10_partitions': [
                {'key': row[partition_col], 'count': row['record_count']}
                for row in top_10
            ]
        }

        logger.info(f"Distribution analysis for {partition_col}:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Total partitions: {total_partitions:,}")
        logger.info(f"  Avg partition size: {avg_partition_size:,.0f}")
        logger.info(f"  Max partition size: {max_partition_size:,}")
        logger.info(f"  Skew ratio: {skew_ratio:.2f}")
        logger.info(f"  Is skewed: {distribution_stats['is_skewed']}")

        if distribution_stats['is_skewed']:
            logger.warning(f"⚠️  SKEW DETECTED in {partition_col}!")
            logger.warning(f"  Top partition has {skew_ratio:.2f}x more data than average")

        self.metrics[f'distribution_{partition_col}'] = distribution_stats

        return distribution_stats

    def apply_salting(self, df, skewed_column: str, salt_factor: int = 10):
        """
        Apply salting technique to handle skewed joins.

        Salting adds a random suffix to skewed keys to distribute them evenly.

        Args:
            df: Input DataFrame
            skewed_column: Column with skew
            salt_factor: Number of salt buckets (default: 10)

        Returns:
            DataFrame with salted column
        """
        logger.info(f"Applying salting to {skewed_column} with factor {salt_factor}")

        # Add salt column with random value 0 to salt_factor-1
        df_salted = df.withColumn(
            "salt",
            (rand() * salt_factor).cast(IntegerType())
        )

        # Create salted key
        df_salted = df_salted.withColumn(
            f"{skewed_column}_salted",
            concat(col(skewed_column), lit("_salt_"), col("salt"))
        )

        logger.info(f"Salted column created: {skewed_column}_salted")

        self.metrics['salting'] = {
            'column': skewed_column,
            'salt_factor': salt_factor,
            'salted_column': f"{skewed_column}_salted"
        }

        return df_salted

    def optimize_partitioning_strategy(self, df, target_partition_size_mb: int = 128):
        """
        Calculate and apply optimal partitioning strategy.

        Args:
            df: Input DataFrame
            target_partition_size_mb: Target size per partition in MB

        Returns:
            Optimally partitioned DataFrame
        """
        logger.info(f"Optimizing partitioning (target: {target_partition_size_mb}MB per partition)")

        # Get current partition count
        current_partitions = df.rdd.getNumPartitions()

        # Estimate total data size
        # This is approximate - in production, use actual file sizes
        row_count = df.count()

        # Estimate average row size (sample-based)
        sample_size = min(10000, row_count)
        sample = df.limit(sample_size)

        # Rough size estimation (this is simplified)
        # In production, use: df.rdd.map(lambda x: len(str(x))).sum()
        estimated_row_size_bytes = 500  # Conservative estimate
        estimated_total_size_mb = (row_count * estimated_row_size_bytes) / (1024 * 1024)

        # Calculate optimal partitions
        optimal_partitions = int(math.ceil(estimated_total_size_mb / target_partition_size_mb))

        # Ensure minimum and maximum bounds
        optimal_partitions = max(200, min(optimal_partitions, 10000))

        logger.info(f"Current partitions: {current_partitions}")
        logger.info(f"Estimated total size: {estimated_total_size_mb:.2f} MB")
        logger.info(f"Optimal partitions: {optimal_partitions}")

        # Apply partitioning
        if abs(current_partitions - optimal_partitions) > current_partitions * 0.2:
            # Significant difference, repartition
            if optimal_partitions < current_partitions:
                df_optimized = df.coalesce(optimal_partitions)
                logger.info(f"Coalesced to {optimal_partitions} partitions")
            else:
                df_optimized = df.repartition(optimal_partitions)
                logger.info(f"Repartitioned to {optimal_partitions} partitions")
        else:
            df_optimized = df
            logger.info("Partitioning is already optimal, no changes needed")

        self.metrics['partition_optimization'] = {
            'current_partitions': current_partitions,
            'optimal_partitions': optimal_partitions,
            'estimated_size_mb': estimated_total_size_mb,
            'target_partition_size_mb': target_partition_size_mb
        }

        return df_optimized

    def handle_skewed_join(
        self,
        df_left,
        df_right,
        join_column: str,
        broadcast_threshold_mb: int = 100
    ):
        """
        Handle skewed joins using multiple techniques.

        Strategies:
        1. Detect if right table is small enough for broadcast
        2. Apply salting if skew detected
        3. Use AQE skew optimization

        Args:
            df_left: Left DataFrame (large, potentially skewed)
            df_right: Right DataFrame
            join_column: Column to join on
            broadcast_threshold_mb: Size threshold for broadcast join in MB

        Returns:
            Joined DataFrame
        """
        logger.info(f"Handling potentially skewed join on {join_column}")

        # Check if right table is small enough for broadcast
        right_count = df_right.count()
        # Rough estimate: 500 bytes per row
        estimated_right_size_mb = (right_count * 500) / (1024 * 1024)

        logger.info(f"Right table estimated size: {estimated_right_size_mb:.2f} MB")

        # Strategy 1: Broadcast join if small enough
        if estimated_right_size_mb <= broadcast_threshold_mb:
            logger.info("✅ Using BROADCAST JOIN (right table is small)")

            df_joined = df_left.join(
                broadcast(df_right),
                on=join_column,
                how="inner"
            )

            self.metrics['skewed_join'] = {
                'strategy': 'broadcast',
                'join_column': join_column,
                'right_table_size_mb': estimated_right_size_mb
            }

            return df_joined

        # Strategy 2: Check for skew in left table
        left_dist = self.analyze_data_distribution(df_left, join_column)

        if left_dist['is_skewed']:
            logger.info("⚠️  Skew detected, applying SALTING technique")

            # Apply salting to both sides
            salt_factor = 10
            df_left_salted = self.apply_salting(df_left, join_column, salt_factor)

            # Replicate right table with all salt values
            from pyspark.sql.functions import explode, array, lit as spark_lit

            # Create array of salt values [0, 1, 2, ..., salt_factor-1]
            salt_values = list(range(salt_factor))

            df_right_replicated = df_right.withColumn(
                "salt_array",
                array([spark_lit(i) for i in salt_values])
            )

            df_right_replicated = df_right_replicated.select(
                "*",
                explode(col("salt_array")).alias("salt")
            ).drop("salt_array")

            # Create salted key for right table
            df_right_replicated = df_right_replicated.withColumn(
                f"{join_column}_salted",
                concat(col(join_column), spark_lit("_salt_"), col("salt"))
            )

            # Perform join on salted keys
            df_joined = df_left_salted.join(
                df_right_replicated,
                on=f"{join_column}_salted",
                how="inner"
            )

            # Drop salt columns
            df_joined = df_joined.drop("salt", f"{join_column}_salted")

            logger.info("✅ Salted join completed")

            self.metrics['skewed_join'] = {
                'strategy': 'salting',
                'join_column': join_column,
                'salt_factor': salt_factor,
                'skew_ratio': left_dist['skew_ratio']
            }

            return df_joined

        # Strategy 3: Regular join with AQE optimization
        logger.info("✅ Using REGULAR JOIN with AQE optimization")

        df_joined = df_left.join(
            df_right,
            on=join_column,
            how="inner"
        )

        self.metrics['skewed_join'] = {
            'strategy': 'aqe_optimized',
            'join_column': join_column
        }

        return df_joined

    def apply_intelligent_partitioning(self, df, partition_columns: list):
        """
        Apply intelligent multi-level partitioning.

        For e-commerce data:
        - Primary: date (year, month, day)
        - Secondary: country or region

        Args:
            df: Input DataFrame
            partition_columns: List of partition columns

        Returns:
            Partitioned DataFrame
        """
        logger.info(f"Applying intelligent partitioning: {partition_columns}")

        # Analyze each partition column
        for col_name in partition_columns:
            if col_name in df.columns:
                self.analyze_data_distribution(df, col_name)

        # Repartition by columns (this prepares for write partitioning)
        df_partitioned = df.repartition(*[col(c) for c in partition_columns])

        logger.info(f"Repartitioned by: {partition_columns}")
        logger.info(f"Final partition count: {df_partitioned.rdd.getNumPartitions()}")

        self.metrics['intelligent_partitioning'] = {
            'partition_columns': partition_columns,
            'final_partitions': df_partitioned.rdd.getNumPartitions()
        }

        return df_partitioned

    def get_metrics(self):
        """Return pipeline metrics."""
        return self.metrics


def main():
    """Main partitioning and skew handling pipeline."""

    # Create Spark session with AQE enabled
    spark = SparkSession.builder \
        .appName("Partitioning-SkewHandling") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
        .getOrCreate()

    logger.info("=" * 80)
    logger.info("STARTING PARTITIONING & SKEW HANDLING PIPELINE")
    logger.info("=" * 80)

    # Initialize handler
    handler = PartitioningSkewHandler(spark)

    # Read silver data
    silver_path = "s3://data-lake/silver/transactions/"
    logger.info(f"Reading data from {silver_path}")

    df = spark.read.parquet(silver_path)

    logger.info(f"Loaded {df.count():,} records")

    # Analyze data distribution for key columns
    handler.analyze_data_distribution(df, "date")
    handler.analyze_data_distribution(df, "device_type")
    handler.analyze_data_distribution(df, "status")

    # Optimize overall partitioning
    df_optimized = handler.optimize_partitioning_strategy(df, target_partition_size_mb=128)

    # Apply intelligent multi-level partitioning
    df_partitioned = handler.apply_intelligent_partitioning(
        df_optimized,
        partition_columns=["year", "month"]
    )

    # Write optimized data
    output_path = "s3://data-lake/silver/transactions_optimized/"
    logger.info(f"Writing optimized data to {output_path}")

    df_partitioned.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .option("compression", "snappy") \
        .save(output_path)

    # Print metrics
    logger.info("=" * 80)
    logger.info("PARTITIONING & SKEW METRICS")
    logger.info("=" * 80)

    import json
    metrics = handler.get_metrics()
    logger.info(json.dumps(metrics, indent=2, default=str))

    logger.info("=" * 80)
    logger.info("PARTITIONING PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
