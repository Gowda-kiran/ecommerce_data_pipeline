"""
Apache Iceberg Partition Evolution Examples
Demonstrates changing partition strategies without data rewrite
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, year, month, day
from datetime import datetime, timedelta
from config.spark_iceberg_config import create_iceberg_spark_session


def setup_partitioned_table(spark):
    """Create initial partitioned table."""

    print("\n" + "="*70)
    print("SETUP: Creating Partitioned Table")
    print("="*70)

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS lakehouse.events")

    # Create table with initial partitioning by day
    spark.sql("""
        CREATE TABLE lakehouse.events (
            event_id STRING,
            user_id STRING,
            event_type STRING,
            event_timestamp TIMESTAMP,
            event_date DATE,
            country STRING,
            city STRING,
            revenue DOUBLE,
            properties MAP<STRING, STRING>
        )
        USING iceberg
        PARTITIONED BY (days(event_date))
    """)

    print("\n✅ Table created with partitioning by days(event_date)")

    # Insert sample data spanning multiple days
    print("\n📝 Inserting sample data...")

    for day_offset in range(30):
        event_date = (datetime(2024, 1, 1) + timedelta(days=day_offset)).strftime('%Y-%m-%d')
        event_timestamp = f"{event_date} 10:00:00"

        spark.sql(f"""
            INSERT INTO lakehouse.events VALUES
            ('E{day_offset:03d}_1', 'U001', 'page_view', TIMESTAMP '{event_timestamp}', DATE '{event_date}', 'US', 'New York', 0.0, MAP('page', '/home')),
            ('E{day_offset:03d}_2', 'U002', 'purchase', TIMESTAMP '{event_timestamp}', DATE '{event_date}', 'UK', 'London', 99.99, MAP('product', 'laptop')),
            ('E{day_offset:03d}_3', 'U003', 'page_view', TIMESTAMP '{event_timestamp}', DATE '{event_date}', 'CA', 'Toronto', 0.0, MAP('page', '/products'))
        """)

    print(f"\n✅ Inserted data for 30 days (90 events)")

    # Show initial partition information
    print("\n📊 Initial Data Sample:")
    spark.sql("SELECT * FROM lakehouse.events ORDER BY event_date LIMIT 10").show(truncate=False)

    # Show partition information
    print("\n📋 Partition Information:")
    spark.sql("SELECT file_path, partition FROM lakehouse.events.files LIMIT 5").show(truncate=False)

    return 90


def example_view_current_partitioning(spark):
    """Example 1: View current partition specification."""

    print("\n" + "="*70)
    print("EXAMPLE 1: VIEW CURRENT PARTITIONING")
    print("="*70)

    print("\n📋 Current Partitions:")
    partitions = spark.sql("""
        SELECT
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as total_records
        FROM lakehouse.events.files
        GROUP BY partition
        ORDER BY partition
    """)
    partitions.show(truncate=False)

    print("\n📋 Partition Spec:")
    spark.sql("DESCRIBE EXTENDED lakehouse.events").show(truncate=False)

    print("\n📊 Files per Partition:")
    spark.sql("""
        SELECT
            partition,
            file_path,
            record_count,
            file_size_in_bytes / 1024 as size_kb
        FROM lakehouse.events.files
        ORDER BY partition
        LIMIT 10
    """).show(truncate=False)


def example_evolve_to_monthly_partitions(spark):
    """Example 2: Evolve from daily to monthly partitions."""

    print("\n" + "="*70)
    print("EXAMPLE 2: EVOLVE TO MONTHLY PARTITIONS")
    print("="*70)

    print("\n🔧 Changing partition spec from days(event_date) to months(event_date)...")

    # Add monthly partition spec
    spark.sql("""
        ALTER TABLE lakehouse.events
        REPLACE PARTITION FIELD days(event_date) WITH months(event_date)
    """)

    print("\n✅ Partition spec changed!")
    print("   • Old data remains in daily partitions")
    print("   • New data will use monthly partitions")
    print("   • No data rewrite required!")

    # Insert new data (will use monthly partitions)
    print("\n📝 Inserting new data (will use monthly partitions)...")
    spark.sql("""
        INSERT INTO lakehouse.events VALUES
        ('E031_1', 'U004', 'page_view', TIMESTAMP '2024-02-01 10:00:00', DATE '2024-02-01', 'US', 'Boston', 0.0, MAP('page', '/about')),
        ('E031_2', 'U005', 'purchase', TIMESTAMP '2024-02-01 11:00:00', DATE '2024-02-01', 'UK', 'Manchester', 149.99, MAP('product', 'phone')),
        ('E032_1', 'U006', 'page_view', TIMESTAMP '2024-02-15 10:00:00', DATE '2024-02-15', 'CA', 'Vancouver', 0.0, MAP('page', '/contact'))
    """)

    print("\n📋 Partition Distribution After Evolution:")
    spark.sql("""
        SELECT
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as total_records
        FROM lakehouse.events.files
        GROUP BY partition
        ORDER BY partition
    """).show(truncate=False)

    print("\n💡 Key Observation:")
    print("   • January data: 30 partitions (daily)")
    print("   • February data: 1 partition (monthly)")
    print("   • Query performance improves for new data!")


def example_multi_dimensional_partitioning(spark):
    """Example 3: Add multi-dimensional partitioning."""

    print("\n" + "="*70)
    print("EXAMPLE 3: MULTI-DIMENSIONAL PARTITIONING")
    print("="*70)

    # Create new table for multi-dimensional example
    spark.sql("DROP TABLE IF EXISTS lakehouse.sales")

    print("\n🔧 Creating table with multi-dimensional partitioning...")
    spark.sql("""
        CREATE TABLE lakehouse.sales (
            sale_id STRING,
            product_id STRING,
            customer_id STRING,
            sale_date DATE,
            region STRING,
            amount DOUBLE,
            quantity INT
        )
        USING iceberg
        PARTITIONED BY (months(sale_date), region)
    """)

    print("\n✅ Table partitioned by: months(sale_date), region")

    # Insert sample data
    print("\n📝 Inserting sample data...")
    spark.sql("""
        INSERT INTO lakehouse.sales VALUES
        ('S001', 'P001', 'C001', DATE '2024-01-15', 'North', 100.00, 2),
        ('S002', 'P002', 'C002', DATE '2024-01-20', 'South', 200.00, 3),
        ('S003', 'P003', 'C003', DATE '2024-01-25', 'North', 150.00, 1),
        ('S004', 'P001', 'C004', DATE '2024-02-10', 'East', 175.00, 2),
        ('S005', 'P002', 'C005', DATE '2024-02-15', 'West', 225.00, 4),
        ('S006', 'P004', 'C006', DATE '2024-02-20', 'North', 300.00, 5)
    """)

    print("\n📋 Multi-Dimensional Partitions:")
    spark.sql("""
        SELECT
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as total_records
        FROM lakehouse.sales.files
        GROUP BY partition
        ORDER BY partition
    """).show(truncate=False)

    print("\n📊 Query with Partition Pruning:")
    print("   Querying: WHERE sale_date >= '2024-02-01' AND region = 'North'")

    result = spark.sql("""
        SELECT *
        FROM lakehouse.sales
        WHERE sale_date >= DATE '2024-02-01'
          AND region = 'North'
    """)

    result.show(truncate=False)

    print("\n✅ Partition pruning eliminates unnecessary partitions!")


def example_hidden_partitioning(spark):
    """Example 4: Demonstrate hidden partitioning."""

    print("\n" + "="*70)
    print("EXAMPLE 4: HIDDEN PARTITIONING")
    print("="*70)

    print("\n💡 What is Hidden Partitioning?")
    print("   • Users don't need to specify partition columns in queries")
    print("   • Iceberg automatically uses partitioning for filtering")
    print("   • No need for 'WHERE partition_column = value'")
    print("   • Partition values are derived from data columns")

    # Query without specifying partition in WHERE clause
    print("\n📊 Query Example 1: Filter by event_date (partition column)")
    print("   Query: WHERE event_date = '2024-01-15'")

    result1 = spark.sql("""
        SELECT event_id, event_type, event_date, country
        FROM lakehouse.events
        WHERE event_date = DATE '2024-01-15'
    """)

    result1.show(truncate=False)
    print("\n✅ Iceberg automatically prunes to the correct partition!")

    # Range query
    print("\n📊 Query Example 2: Range query on partition column")
    print("   Query: WHERE event_date BETWEEN '2024-01-10' AND '2024-01-15'")

    result2 = spark.sql("""
        SELECT COUNT(*) as event_count, MIN(event_date) as min_date, MAX(event_date) as max_date
        FROM lakehouse.events
        WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-15'
    """)

    result2.show(truncate=False)
    print("\n✅ Multiple partitions scanned efficiently!")

    # Show query plan for partition pruning
    print("\n📋 Query Execution Plan (showing partition filters):")
    spark.sql("""
        SELECT *
        FROM lakehouse.events
        WHERE event_date = DATE '2024-01-15'
    """).explain(True)


def example_partition_transforms(spark):
    """Example 5: Different partition transform functions."""

    print("\n" + "="*70)
    print("EXAMPLE 5: PARTITION TRANSFORMS")
    print("="*70)

    # Create table demonstrating different transforms
    spark.sql("DROP TABLE IF EXISTS lakehouse.transform_demo")

    print("\n🔧 Creating table with various partition transforms...")
    spark.sql("""
        CREATE TABLE lakehouse.transform_demo (
            id STRING,
            timestamp_col TIMESTAMP,
            date_col DATE,
            string_col STRING,
            value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (
            years(timestamp_col),
            months(date_col),
            bucket(10, string_col)
        )
    """)

    print("\n✅ Partition transforms applied:")
    print("   • years(timestamp_col) - Extracts year from timestamp")
    print("   • months(date_col) - Extracts year-month from date")
    print("   • bucket(10, string_col) - Hash bucket (0-9)")

    # Insert sample data
    spark.sql("""
        INSERT INTO lakehouse.transform_demo VALUES
        ('1', TIMESTAMP '2023-06-15 10:00:00', DATE '2024-01-15', 'alpha', 100.0),
        ('2', TIMESTAMP '2023-12-20 11:00:00', DATE '2024-01-20', 'beta', 200.0),
        ('3', TIMESTAMP '2024-03-10 09:00:00', DATE '2024-02-10', 'gamma', 150.0),
        ('4', TIMESTAMP '2024-06-05 14:00:00', DATE '2024-02-15', 'delta', 175.0)
    """)

    print("\n📋 Resulting Partitions:")
    spark.sql("""
        SELECT
            partition,
            record_count
        FROM lakehouse.transform_demo.files
        ORDER BY partition
    """).show(truncate=False)

    print("\n📚 Available Partition Transforms:")
    print("   • years(col) - Year transform")
    print("   • months(col) - Year-month transform")
    print("   • days(col) - Year-month-day transform")
    print("   • hours(col) - Year-month-day-hour transform")
    print("   • bucket(N, col) - Hash to N buckets")
    print("   • truncate(L, col) - Truncate strings/numbers to length L")


def example_add_partition_field(spark):
    """Example 6: Add new partition field to existing table."""

    print("\n" + "="*70)
    print("EXAMPLE 6: ADD PARTITION FIELD")
    print("="*70)

    print("\n🔧 Adding 'country' as an additional partition field...")
    spark.sql("""
        ALTER TABLE lakehouse.events
        ADD PARTITION FIELD country
    """)

    print("\n✅ New partition field added!")
    print("   • Existing data stays in original partitions")
    print("   • New data will use both event_date AND country partitions")

    # Insert new data
    print("\n📝 Inserting new data (will use new partition spec)...")
    spark.sql("""
        INSERT INTO lakehouse.events VALUES
        ('E100_1', 'U010', 'purchase', TIMESTAMP '2024-03-01 10:00:00', DATE '2024-03-01', 'US', 'Seattle', 299.99, MAP('product', 'tablet')),
        ('E100_2', 'U011', 'purchase', TIMESTAMP '2024-03-01 11:00:00', DATE '2024-03-01', 'UK', 'Edinburgh', 199.99, MAP('product', 'headphones')),
        ('E100_3', 'U012', 'page_view', TIMESTAMP '2024-03-01 12:00:00', DATE '2024-03-01', 'US', 'Austin', 0.0, MAP('page', '/deals'))
    """)

    print("\n📋 Partition Distribution After Adding Field:")
    spark.sql("""
        SELECT
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as total_records
        FROM lakehouse.events.files
        GROUP BY partition
        ORDER BY partition DESC
        LIMIT 15
    """).show(truncate=False)


def example_remove_partition_field(spark):
    """Example 7: Remove partition field."""

    print("\n" + "="*70)
    print("EXAMPLE 7: REMOVE PARTITION FIELD")
    print("="*70)

    # Create simple table
    spark.sql("DROP TABLE IF EXISTS lakehouse.remove_demo")

    spark.sql("""
        CREATE TABLE lakehouse.remove_demo (
            id STRING,
            category STRING,
            created_date DATE,
            value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (category, days(created_date))
    """)

    # Insert data
    spark.sql("""
        INSERT INTO lakehouse.remove_demo VALUES
        ('1', 'A', DATE '2024-01-15', 100.0),
        ('2', 'B', DATE '2024-01-16', 200.0),
        ('3', 'A', DATE '2024-01-17', 150.0)
    """)

    print("\n📋 Initial Partitions:")
    spark.sql("""
        SELECT partition, record_count
        FROM lakehouse.remove_demo.files
    """).show(truncate=False)

    print("\n🔧 Removing 'category' partition field...")
    spark.sql("""
        ALTER TABLE lakehouse.remove_demo
        DROP PARTITION FIELD category
    """)

    print("\n✅ Partition field removed!")
    print("   • Old data remains in category partitions")
    print("   • New data will only use days(created_date) partition")

    # Insert new data
    spark.sql("""
        INSERT INTO lakehouse.remove_demo VALUES
        ('4', 'C', DATE '2024-01-18', 175.0)
    """)

    print("\n📋 Partitions After Removal:")
    spark.sql("""
        SELECT partition, record_count
        FROM lakehouse.remove_demo.files
        ORDER BY partition
    """).show(truncate=False)


def example_partition_evolution_history(spark):
    """Example 8: View partition evolution history."""

    print("\n" + "="*70)
    print("EXAMPLE 8: PARTITION EVOLUTION HISTORY")
    print("="*70)

    print("\n📋 Table Snapshots (showing partition spec changes):")
    spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            summary
        FROM lakehouse.events.snapshots
        ORDER BY committed_at DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n📋 Partition Spec History:")
    print("   Iceberg maintains full history of partition specifications")
    print("   Each snapshot references its partition spec")
    print("   Time travel queries use the correct partition spec for that point in time")

    # Get metadata
    print("\n📋 Current Partition Fields:")
    spark.sql("""
        SELECT
            partition_id,
            spec_id,
            name,
            transform
        FROM lakehouse.events.partition_fields
        ORDER BY partition_id
    """).show(truncate=False)


def example_partition_statistics(spark):
    """Example 9: Analyze partition statistics and performance."""

    print("\n" + "="*70)
    print("EXAMPLE 9: PARTITION STATISTICS")
    print("="*70)

    print("\n📊 Partition Size Analysis:")
    spark.sql("""
        SELECT
            partition,
            COUNT(*) as num_files,
            SUM(record_count) as total_records,
            SUM(file_size_in_bytes) / (1024 * 1024) as total_size_mb,
            AVG(file_size_in_bytes) / 1024 as avg_file_size_kb
        FROM lakehouse.events.files
        GROUP BY partition
        ORDER BY total_records DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n📊 Overall Statistics:")
    stats = spark.sql("""
        SELECT
            COUNT(DISTINCT partition) as total_partitions,
            COUNT(*) as total_files,
            SUM(record_count) as total_records,
            SUM(file_size_in_bytes) / (1024 * 1024) as total_size_mb
        FROM lakehouse.events.files
    """)
    stats.show(truncate=False)

    print("\n💡 Partition Health Indicators:")
    print("   ✅ Balanced partition sizes")
    print("   ✅ Reasonable number of files per partition")
    print("   ✅ Consistent record counts across partitions")
    print("   ⚠️  Watch for: Too many small files, uneven distribution")


def example_partition_best_practices(spark):
    """Example 10: Partition best practices and recommendations."""

    print("\n" + "="*70)
    print("EXAMPLE 10: PARTITION BEST PRACTICES")
    print("="*70)

    print("\n📚 PARTITIONING BEST PRACTICES:")

    print("\n1️⃣  CARDINALITY:")
    print("   ✅ Aim for 100MB - 1GB partitions")
    print("   ✅ Avoid high cardinality (millions of partitions)")
    print("   ✅ Avoid low cardinality (too few partitions)")
    print("   Example: Partition by month, not by second")

    print("\n2️⃣  QUERY PATTERNS:")
    print("   ✅ Partition by columns frequently used in WHERE clauses")
    print("   ✅ Consider time-based partitioning for time-series data")
    print("   ✅ Use bucket() for even distribution on high-cardinality columns")

    print("\n3️⃣  PARTITION EVOLUTION:")
    print("   ✅ Start with coarse partitions (e.g., yearly)")
    print("   ✅ Evolve to finer partitions as data grows (e.g., monthly, daily)")
    print("   ✅ No need to rewrite historical data")

    print("\n4️⃣  HIDDEN PARTITIONING:")
    print("   ✅ Use partition transforms (years, months, days, bucket)")
    print("   ✅ No need to expose partition columns to users")
    print("   ✅ Iceberg handles partition pruning automatically")

    print("\n5️⃣  MULTI-DIMENSIONAL:")
    print("   ✅ Combine time + categorical dimensions")
    print("   ✅ Example: PARTITIONED BY (months(date), region)")
    print("   ⚠️  Avoid too many dimensions (increases partition count)")

    print("\n6️⃣  MONITORING:")
    print("   ✅ Monitor partition sizes regularly")
    print("   ✅ Watch for small file problems")
    print("   ✅ Use table maintenance procedures (compaction)")

    # Example of good vs bad partitioning
    print("\n\n📊 EXAMPLE COMPARISON:")

    print("\n❌ Bad Partitioning:")
    print("   PARTITIONED BY (user_id)")
    print("   Problem: Too many partitions (one per user)")

    print("\n✅ Good Partitioning:")
    print("   PARTITIONED BY (months(created_date), bucket(100, user_id))")
    print("   Benefit: Time-based + distributed by user")

    print("\n❌ Bad Partitioning:")
    print("   PARTITIONED BY (timestamp)")
    print("   Problem: Extreme cardinality (one per timestamp)")

    print("\n✅ Good Partitioning:")
    print("   PARTITIONED BY (hours(timestamp))")
    print("   Benefit: Granular but reasonable cardinality")


def main():
    """Main execution function."""

    # Create Spark session
    spark = create_iceberg_spark_session(
        app_name="Iceberg Partition Evolution Examples"
    )

    try:
        # Setup partitioned table
        record_count = setup_partitioned_table(spark)
        print(f"\n✅ Initial table created with {record_count} records")

        # Example 1: View current partitioning
        example_view_current_partitioning(spark)

        # Example 2: Evolve to monthly partitions
        example_evolve_to_monthly_partitions(spark)

        # Example 3: Multi-dimensional partitioning
        example_multi_dimensional_partitioning(spark)

        # Example 4: Hidden partitioning
        example_hidden_partitioning(spark)

        # Example 5: Partition transforms
        example_partition_transforms(spark)

        # Example 6: Add partition field
        example_add_partition_field(spark)

        # Example 7: Remove partition field
        example_remove_partition_field(spark)

        # Example 8: Partition evolution history
        example_partition_evolution_history(spark)

        # Example 9: Partition statistics
        example_partition_statistics(spark)

        # Example 10: Best practices
        example_partition_best_practices(spark)

        print("\n" + "="*70)
        print("✅ ALL PARTITION EVOLUTION EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*70)

        print("\n📚 KEY TAKEAWAYS:")
        print("   ✅ Change partitioning without rewriting data")
        print("   ✅ Hidden partitioning - no user code changes")
        print("   ✅ Multiple partition transforms (years, months, days, bucket)")
        print("   ✅ Add/remove partition fields dynamically")
        print("   ✅ Multi-dimensional partitioning")
        print("   ✅ Automatic partition pruning")
        print("   ✅ Full partition evolution history")
        print("   ✅ No downtime for partition changes")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
