"""
Apache Iceberg Time Travel Examples
Demonstrates querying historical data using snapshots and timestamps
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, current_timestamp
from datetime import datetime, timedelta
from config.spark_iceberg_config import create_iceberg_spark_session


def setup_sample_data(spark):
    """Create sample data with multiple versions for time travel demo."""

    # Create database and table
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS lakehouse.sales")

    # Create table
    spark.sql("""
        CREATE TABLE lakehouse.sales (
            sale_id STRING,
            product_id STRING,
            quantity INT,
            amount DOUBLE,
            sale_date DATE,
            region STRING
        )
        USING iceberg
        PARTITIONED BY (days(sale_date))
    """)

    print("✅ Sample table created")

    # Insert initial data (Version 1)
    print("\n📝 Inserting initial data (Snapshot 1)...")
    spark.sql("""
        INSERT INTO lakehouse.sales VALUES
        ('S001', 'P001', 10, 100.00, DATE '2024-01-01', 'North'),
        ('S002', 'P002', 5, 50.00, DATE '2024-01-01', 'South'),
        ('S003', 'P001', 8, 80.00, DATE '2024-01-02', 'East'),
        ('S004', 'P003', 15, 150.00, DATE '2024-01-02', 'West')
    """)

    snapshot1_time = datetime.now()
    print(f"Snapshot 1 timestamp: {snapshot1_time}")

    # Wait a moment
    import time
    time.sleep(2)

    # Insert more data (Version 2)
    print("\n📝 Inserting more data (Snapshot 2)...")
    spark.sql("""
        INSERT INTO lakehouse.sales VALUES
        ('S005', 'P002', 20, 200.00, DATE '2024-01-03', 'North'),
        ('S006', 'P001', 12, 120.00, DATE '2024-01-03', 'South')
    """)

    snapshot2_time = datetime.now()
    print(f"Snapshot 2 timestamp: {snapshot2_time}")

    time.sleep(2)

    # Update some records (Version 3)
    print("\n📝 Updating records (Snapshot 3)...")
    spark.sql("""
        UPDATE lakehouse.sales
        SET amount = amount * 1.1
        WHERE region = 'North'
    """)

    snapshot3_time = datetime.now()
    print(f"Snapshot 3 timestamp: {snapshot3_time}")

    time.sleep(2)

    # Delete some records (Version 4)
    print("\n📝 Deleting records (Snapshot 4)...")
    spark.sql("""
        DELETE FROM lakehouse.sales
        WHERE quantity < 10
    """)

    snapshot4_time = datetime.now()
    print(f"Snapshot 4 timestamp: {snapshot4_time}")

    return {
        'snapshot1': snapshot1_time,
        'snapshot2': snapshot2_time,
        'snapshot3': snapshot3_time,
        'snapshot4': snapshot4_time
    }


def example_time_travel_by_timestamp(spark, timestamps):
    """Query table at different points in time using timestamps."""

    print("\n" + "="*70)
    print("EXAMPLE 1: TIME TRAVEL BY TIMESTAMP")
    print("="*70)

    # Current state
    print("\n📊 CURRENT STATE:")
    current = spark.sql("SELECT * FROM lakehouse.sales ORDER BY sale_id")
    current.show()
    print(f"Total rows: {current.count()}")

    # Query as of Snapshot 1
    print(f"\n📊 STATE AT SNAPSHOT 1 ({timestamps['snapshot1']}):")
    snapshot1_df = spark.read \
        .option("as-of-timestamp", timestamps['snapshot1'].strftime("%Y-%m-%d %H:%M:%S")) \
        .table("lakehouse.sales")
    snapshot1_df.show()
    print(f"Total rows: {snapshot1_df.count()}")

    # Query as of Snapshot 2
    print(f"\n📊 STATE AT SNAPSHOT 2 ({timestamps['snapshot2']}):")
    snapshot2_df = spark.read \
        .option("as-of-timestamp", timestamps['snapshot2'].strftime("%Y-%m-%d %H:%M:%S")) \
        .table("lakehouse.sales")
    snapshot2_df.show()
    print(f"Total rows: {snapshot2_df.count()}")


def example_time_travel_by_snapshot_id(spark):
    """Query table using specific snapshot IDs."""

    print("\n" + "="*70)
    print("EXAMPLE 2: TIME TRAVEL BY SNAPSHOT ID")
    print("="*70)

    # Get all snapshots
    snapshots = spark.sql("SELECT * FROM lakehouse.sales.snapshots ORDER BY committed_at")
    print("\n📋 All Snapshots:")
    snapshots.show(truncate=False)

    # Get snapshot IDs
    snapshot_ids = [row.snapshot_id for row in snapshots.collect()]

    if len(snapshot_ids) >= 2:
        # Query first snapshot
        print(f"\n📊 FIRST SNAPSHOT (ID: {snapshot_ids[0]}):")
        first_snapshot = spark.read \
            .option("snapshot-id", snapshot_ids[0]) \
            .table("lakehouse.sales")
        first_snapshot.show()

        # Query second snapshot
        print(f"\n📊 SECOND SNAPSHOT (ID: {snapshot_ids[1]}):")
        second_snapshot = spark.read \
            .option("snapshot-id", snapshot_ids[1]) \
            .table("lakehouse.sales")
        second_snapshot.show()


def example_compare_snapshots(spark):
    """Compare data between different snapshots."""

    print("\n" + "="*70)
    print("EXAMPLE 3: COMPARE SNAPSHOTS")
    print("="*70)

    # Get snapshot IDs
    snapshots = spark.sql("SELECT snapshot_id FROM lakehouse.sales.snapshots ORDER BY committed_at")
    snapshot_ids = [row.snapshot_id for row in snapshots.collect()]

    if len(snapshot_ids) >= 2:
        # Read two different snapshots
        snapshot1 = spark.read \
            .option("snapshot-id", snapshot_ids[0]) \
            .table("lakehouse.sales")

        snapshot2 = spark.read \
            .option("snapshot-id", snapshot_ids[-1]) \
            .table("lakehouse.sales")

        # Compare metrics
        print("\n📊 METRICS COMPARISON:")
        print("\nSnapshot 1 Metrics:")
        snapshot1.agg(
            count("*").alias("total_rows"),
            _sum("amount").alias("total_amount"),
            avg("quantity").alias("avg_quantity")
        ).show()

        print("Current Snapshot Metrics:")
        snapshot2.agg(
            count("*").alias("total_rows"),
            _sum("amount").alias("total_amount"),
            avg("quantity").alias("avg_quantity")
        ).show()

        # Find differences
        print("\n🔍 RECORDS IN CURRENT BUT NOT IN FIRST SNAPSHOT:")
        diff = snapshot2.subtract(snapshot1)
        diff.show()


def example_incremental_processing(spark):
    """Process only changes between snapshots."""

    print("\n" + "="*70)
    print("EXAMPLE 4: INCREMENTAL PROCESSING")
    print("="*70)

    # Get snapshots
    snapshots = spark.sql("""
        SELECT snapshot_id, committed_at
        FROM lakehouse.sales.snapshots
        ORDER BY committed_at
    """)

    print("\n📋 Available Snapshots:")
    snapshots.show(truncate=False)

    snapshot_list = snapshots.collect()

    if len(snapshot_list) >= 2:
        # Read incrementally from first to second snapshot
        start_snapshot = snapshot_list[0].snapshot_id
        end_snapshot = snapshot_list[1].snapshot_id

        print(f"\n📊 INCREMENTAL READ:")
        print(f"From snapshot: {start_snapshot}")
        print(f"To snapshot: {end_snapshot}")

        incremental_df = spark.read \
            .format("iceberg") \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .load("lakehouse.sales")

        print("\nNew/Changed records:")
        incremental_df.show()


def example_rollback_to_snapshot(spark):
    """Rollback table to a previous snapshot."""

    print("\n" + "="*70)
    print("EXAMPLE 5: ROLLBACK TO PREVIOUS SNAPSHOT")
    print("="*70)

    # Get snapshots
    snapshots = spark.sql("SELECT snapshot_id, committed_at FROM lakehouse.sales.snapshots ORDER BY committed_at")
    snapshot_list = snapshots.collect()

    print("\n📋 Current Snapshots:")
    snapshots.show(truncate=False)

    # Show current state
    print("\n📊 CURRENT STATE:")
    current_count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.sales").collect()[0].count
    print(f"Total rows: {current_count}")

    if len(snapshot_list) >= 2:
        # Rollback to first snapshot
        rollback_snapshot = snapshot_list[0].snapshot_id
        print(f"\n⏮️  Rolling back to snapshot: {rollback_snapshot}")

        spark.sql(f"""
            CALL iceberg.system.rollback_to_snapshot(
                'lakehouse.sales',
                {rollback_snapshot}
            )
        """)

        print("\n📊 STATE AFTER ROLLBACK:")
        rollback_count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.sales").collect()[0].count
        print(f"Total rows: {rollback_count}")

        spark.sql("SELECT * FROM lakehouse.sales ORDER BY sale_id").show()


def example_snapshot_expiration(spark):
    """Demonstrate snapshot expiration for cleanup."""

    print("\n" + "="*70)
    print("EXAMPLE 6: SNAPSHOT EXPIRATION")
    print("="*70)

    # Show all snapshots
    print("\n📋 Snapshots Before Expiration:")
    before = spark.sql("SELECT snapshot_id, committed_at FROM lakehouse.sales.snapshots ORDER BY committed_at")
    before.show(truncate=False)
    before_count = before.count()

    # Expire old snapshots (keep last 2)
    print("\n🗑️  Expiring old snapshots (keeping last 2)...")

    spark.sql(f"""
        CALL iceberg.system.expire_snapshots(
            table => 'lakehouse.sales',
            retain_last => 2
        )
    """)

    # Show remaining snapshots
    print("\n📋 Snapshots After Expiration:")
    after = spark.sql("SELECT snapshot_id, committed_at FROM lakehouse.sales.snapshots ORDER BY committed_at")
    after.show(truncate=False)
    after_count = after.count()

    print(f"\n📊 Expired {before_count - after_count} snapshot(s)")


def example_snapshot_metadata(spark):
    """Explore snapshot metadata."""

    print("\n" + "="*70)
    print("EXAMPLE 7: SNAPSHOT METADATA")
    print("="*70)

    # Detailed snapshot information
    print("\n📋 DETAILED SNAPSHOT INFORMATION:")
    spark.sql("""
        SELECT
            snapshot_id,
            parent_id,
            operation,
            committed_at,
            summary
        FROM lakehouse.sales.snapshots
        ORDER BY committed_at DESC
    """).show(truncate=False)

    # Snapshot files
    print("\n📁 SNAPSHOT FILES:")
    spark.sql("""
        SELECT
            file_path,
            file_format,
            record_count,
            file_size_in_bytes
        FROM lakehouse.sales.files
        LIMIT 10
    """).show(truncate=False)

    # Table history
    print("\n📜 TABLE HISTORY:")
    spark.sql("""
        SELECT
            made_current_at,
            snapshot_id,
            is_current_ancestor
        FROM lakehouse.sales.history
        ORDER BY made_current_at DESC
    """).show(truncate=False)


def main():
    """Main execution function."""

    # Create Spark session
    spark = create_iceberg_spark_session(
        app_name="Iceberg Time Travel Examples"
    )

    try:
        # Setup sample data with multiple versions
        timestamps = setup_sample_data(spark)

        # Example 1: Time travel by timestamp
        example_time_travel_by_timestamp(spark, timestamps)

        # Example 2: Time travel by snapshot ID
        example_time_travel_by_snapshot_id(spark)

        # Example 3: Compare snapshots
        example_compare_snapshots(spark)

        # Example 4: Incremental processing
        example_incremental_processing(spark)

        # Example 5: Rollback
        example_rollback_to_snapshot(spark)

        # Example 6: Expire snapshots
        example_snapshot_expiration(spark)

        # Example 7: Snapshot metadata
        example_snapshot_metadata(spark)

        print("\n" + "="*70)
        print("✅ ALL TIME TRAVEL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
