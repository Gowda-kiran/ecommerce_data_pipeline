"""
Apache Iceberg Performance Optimization Examples
Demonstrates table maintenance, compaction, and performance tuning
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, current_timestamp
from datetime import datetime, timedelta
from config.spark_iceberg_config import create_iceberg_spark_session


def setup_performance_test_table(spark):
    """Create table for performance testing."""

    print("\n" + "="*70)
    print("SETUP: Creating Performance Test Table")
    print("="*70)

    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")
    spark.sql("DROP TABLE IF EXISTS lakehouse.perf_test")

    # Create table
    spark.sql("""
        CREATE TABLE lakehouse.perf_test (
            id STRING,
            category STRING,
            value DOUBLE,
            event_date DATE,
            created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_date))
        TBLPROPERTIES (
            'write.target-file-size-bytes'='134217728',  -- 128 MB
            'write.metadata.delete-after-commit.enabled'='true',
            'write.metadata.previous-versions-max'='5'
        )
    """)

    print("\n✅ Performance test table created")

    # Insert data to create multiple small files (simulating small file problem)
    print("\n📝 Inserting data in small batches...")

    for i in range(20):
        event_date = (datetime(2024, 1, 1) + timedelta(days=i % 5)).strftime('%Y-%m-%d')
        spark.sql(f"""
            INSERT INTO lakehouse.perf_test VALUES
            ('ID{i:03d}', 'Category{i % 3}', {i * 10.5}, DATE '{event_date}', CURRENT_TIMESTAMP)
        """)

    print(f"\n✅ Inserted 20 small batches (creates many small files)")

    return 20


def example_small_files_problem(spark):
    """Example 1: Demonstrate the small files problem."""

    print("\n" + "="*70)
    print("EXAMPLE 1: SMALL FILES PROBLEM")
    print("="*70)

    print("\n📊 Analyzing file sizes...")

    files_df = spark.sql("""
        SELECT
            file_path,
            file_size_in_bytes,
            file_size_in_bytes / 1024 as size_kb,
            record_count,
            partition
        FROM lakehouse.perf_test.files
        ORDER BY file_size_in_bytes
    """)

    files_df.show(truncate=False)

    # Get statistics
    stats = spark.sql("""
        SELECT
            COUNT(*) as total_files,
            SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb,
            AVG(file_size_in_bytes) / 1024 as avg_size_kb,
            MIN(file_size_in_bytes) / 1024 as min_size_kb,
            MAX(file_size_in_bytes) / 1024 as max_size_kb,
            SUM(record_count) as total_records
        FROM lakehouse.perf_test.files
    """).collect()[0]

    print(f"\n📊 File Statistics:")
    print(f"   Total Files: {stats.total_files}")
    print(f"   Total Size: {stats.total_size_mb:.2f} MB")
    print(f"   Average File Size: {stats.avg_size_kb:.2f} KB")
    print(f"   Min File Size: {stats.min_size_kb:.2f} KB")
    print(f"   Max File Size: {stats.max_size_kb:.2f} KB")
    print(f"   Total Records: {stats.total_records}")

    print("\n⚠️  SMALL FILES PROBLEM:")
    print("   • Many small files increase metadata overhead")
    print("   • Slower query planning")
    print("   • Reduced parallelism efficiency")
    print("   • Higher storage costs (more objects)")

    return stats


def example_compaction(spark):
    """Example 2: Compact small files into larger files."""

    print("\n" + "="*70)
    print("EXAMPLE 2: FILE COMPACTION")
    print("="*70)

    # Get before statistics
    before_stats = spark.sql("""
        SELECT COUNT(*) as file_count, SUM(record_count) as record_count
        FROM lakehouse.perf_test.files
    """).collect()[0]

    print(f"\n📊 Before Compaction:")
    print(f"   Files: {before_stats.file_count}")
    print(f"   Records: {before_stats.record_count}")

    # Run compaction using rewrite_data_files procedure
    print("\n🔧 Running compaction...")

    spark.sql("""
        CALL iceberg.system.rewrite_data_files(
            table => 'lakehouse.perf_test',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '134217728',  -- 128 MB
                'min-file-size-bytes', '1048576'  -- 1 MB
            )
        )
    """)

    # Get after statistics
    after_stats = spark.sql("""
        SELECT COUNT(*) as file_count, SUM(record_count) as record_count
        FROM lakehouse.perf_test.files
    """).collect()[0]

    print(f"\n📊 After Compaction:")
    print(f"   Files: {after_stats.file_count}")
    print(f"   Records: {after_stats.record_count}")

    improvement = ((before_stats.file_count - after_stats.file_count) / before_stats.file_count * 100) if before_stats.file_count > 0 else 0

    print(f"\n✅ Compaction Results:")
    print(f"   Files Reduced: {before_stats.file_count} → {after_stats.file_count}")
    print(f"   Improvement: {improvement:.1f}%")
    print(f"   Records Preserved: {after_stats.record_count}")

    print("\n💡 Compaction Benefits:")
    print("   ✅ Fewer files to track")
    print("   ✅ Faster query planning")
    print("   ✅ Better compression")
    print("   ✅ Improved scan performance")


def example_partition_level_compaction(spark):
    """Example 3: Compact specific partitions."""

    print("\n" + "="*70)
    print("EXAMPLE 3: PARTITION-LEVEL COMPACTION")
    print("="*70)

    # Show partition file distribution
    print("\n📊 Files per Partition:")
    spark.sql("""
        SELECT
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as record_count,
            SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb
        FROM lakehouse.perf_test.files
        GROUP BY partition
        ORDER BY partition
    """).show(truncate=False)

    # Compact only one partition
    print("\n🔧 Compacting specific partition...")

    # Note: In production, you'd specify the exact partition
    print("   Example: CALL rewrite_data_files WHERE partition spec matches")

    print("\n💡 Partition-Level Benefits:")
    print("   ✅ Optimize only hot partitions")
    print("   ✅ Reduce maintenance window")
    print("   ✅ Lower resource usage")
    print("   ✅ Targeted optimization")


def example_snapshot_expiration(spark):
    """Example 4: Expire old snapshots to reduce metadata."""

    print("\n" + "="*70)
    print("EXAMPLE 4: SNAPSHOT EXPIRATION")
    print("="*70)

    # Show all snapshots
    print("\n📋 Snapshots Before Expiration:")
    before_snapshots = spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            operation
        FROM lakehouse.perf_test.snapshots
        ORDER BY committed_at
    """)
    before_snapshots.show(truncate=False)
    before_count = before_snapshots.count()

    # Expire old snapshots (keep last 5)
    print("\n🔧 Expiring old snapshots (keeping last 5)...")

    spark.sql("""
        CALL iceberg.system.expire_snapshots(
            table => 'lakehouse.perf_test',
            retain_last => 5,
            older_than => TIMESTAMP '2099-01-01 00:00:00'
        )
    """)

    # Show remaining snapshots
    print("\n📋 Snapshots After Expiration:")
    after_snapshots = spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            operation
        FROM lakehouse.perf_test.snapshots
        ORDER BY committed_at
    """)
    after_snapshots.show(truncate=False)
    after_count = after_snapshots.count()

    print(f"\n✅ Expiration Results:")
    print(f"   Snapshots Before: {before_count}")
    print(f"   Snapshots After: {after_count}")
    print(f"   Expired: {before_count - after_count}")

    print("\n💡 Snapshot Expiration Benefits:")
    print("   ✅ Reduced metadata overhead")
    print("   ✅ Faster metadata operations")
    print("   ✅ Lower storage costs")
    print("   ✅ Simplified history management")


def example_orphan_file_removal(spark):
    """Example 5: Remove orphaned files."""

    print("\n" + "="*70)
    print("EXAMPLE 5: ORPHAN FILE REMOVAL")
    print("="*70)

    print("\n💡 Orphan files are data files not referenced by any snapshot")
    print("   They can accumulate from failed writes or expired snapshots")

    # Show metadata files
    print("\n📁 Current Metadata Files:")
    spark.sql("""
        SELECT file_path, file_size_in_bytes / 1024 as size_kb
        FROM lakehouse.perf_test.files
        LIMIT 5
    """).show(truncate=False)

    # Remove orphan files (files older than 3 days not in any snapshot)
    print("\n🔧 Removing orphan files...")

    # Note: This procedure removes files not referenced by snapshots
    spark.sql("""
        CALL iceberg.system.remove_orphan_files(
            table => 'lakehouse.perf_test',
            older_than => TIMESTAMP '2024-01-01 00:00:00'
        )
    """)

    print("\n✅ Orphan file removal complete")

    print("\n💡 Benefits:")
    print("   ✅ Reclaim storage space")
    print("   ✅ Clean up failed writes")
    print("   ✅ Reduce storage costs")
    print("   ✅ Maintain clean data lake")


def example_metadata_cleanup(spark):
    """Example 6: Clean up old metadata files."""

    print("\n" + "="*70)
    print("EXAMPLE 6: METADATA FILE CLEANUP")
    print("="*70)

    print("\n📋 Table Metadata Files:")
    spark.sql("SHOW TBLPROPERTIES lakehouse.perf_test").show(truncate=False)

    # Show metadata log entries
    print("\n📋 Metadata Log Entries:")
    spark.sql("""
        SELECT
            file,
            latest_snapshot_id,
            latest_schema_id
        FROM lakehouse.perf_test.metadata_log_entries
        ORDER BY timestamp DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n💡 Metadata Cleanup Best Practices:")
    print("   ✅ Set write.metadata.delete-after-commit.enabled=true")
    print("   ✅ Set write.metadata.previous-versions-max=5")
    print("   ✅ Regularly expire snapshots")
    print("   ✅ Remove orphan files periodically")


def example_query_optimization(spark):
    """Example 7: Query optimization techniques."""

    print("\n" + "="*70)
    print("EXAMPLE 7: QUERY OPTIMIZATION")
    print("="*70)

    # Example 1: Partition pruning
    print("\n💡 Optimization 1: Partition Pruning")
    print("   Query with partition filter:")

    query1 = """
        SELECT COUNT(*) as count
        FROM lakehouse.perf_test
        WHERE event_date = DATE '2024-01-01'
    """

    print(f"   {query1}")
    result1 = spark.sql(query1)
    result1.show()

    print("\n   Execution plan shows partition filter:")
    result1.explain(True)

    # Example 2: Column pruning
    print("\n💡 Optimization 2: Column Pruning")
    print("   Select only needed columns:")

    query2 = """
        SELECT id, value
        FROM lakehouse.perf_test
        WHERE category = 'Category1'
        LIMIT 5
    """

    print(f"   {query2}")
    spark.sql(query2).show()

    # Example 3: Predicate pushdown
    print("\n💡 Optimization 3: Predicate Pushdown")
    print("   Filters pushed to file level:")

    query3 = """
        SELECT *
        FROM lakehouse.perf_test
        WHERE value > 50.0
        LIMIT 5
    """

    print(f"   {query3}")
    spark.sql(query3).show()

    print("\n✅ Query Optimization Techniques:")
    print("   ✅ Use partition filters")
    print("   ✅ Select only needed columns")
    print("   ✅ Leverage predicate pushdown")
    print("   ✅ Use metadata for statistics")


def example_table_properties_tuning(spark):
    """Example 8: Tune table properties for performance."""

    print("\n" + "="*70)
    print("EXAMPLE 8: TABLE PROPERTIES TUNING")
    print("="*70)

    # Create optimized table
    spark.sql("DROP TABLE IF EXISTS lakehouse.optimized_table")

    print("\n🔧 Creating table with optimized properties...")

    spark.sql("""
        CREATE TABLE lakehouse.optimized_table (
            id STRING,
            data STRING,
            value DOUBLE,
            created_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            -- File size tuning
            'write.target-file-size-bytes'='536870912',  -- 512 MB
            'write.parquet.compression-codec'='zstd',
            'write.parquet.compression-level'='3',

            -- Metadata optimization
            'write.metadata.delete-after-commit.enabled'='true',
            'write.metadata.previous-versions-max'='10',

            -- Snapshot management
            'history.expire.max-snapshot-age-ms'='604800000',  -- 7 days

            -- Performance
            'read.split.target-size'='134217728',  -- 128 MB
            'read.parquet.vectorization.enabled'='true',

            -- Compaction
            'write.data.files.max'='100',
            'write.manifest.target-file-size-bytes'='8388608'  -- 8 MB
        )
    """)

    print("\n📋 Optimized Table Properties:")
    spark.sql("SHOW TBLPROPERTIES lakehouse.optimized_table").show(truncate=False)

    print("\n✅ Key Performance Properties:")
    print("   • Target file size: Controls compaction")
    print("   • Compression codec: ZSTD balance speed/compression")
    print("   • Split size: Controls parallelism")
    print("   • Vectorization: Faster column reads")
    print("   • Metadata cleanup: Reduces overhead")


def example_caching_strategies(spark):
    """Example 9: Caching and materialization strategies."""

    print("\n" + "="*70)
    print("EXAMPLE 9: CACHING STRATEGIES")
    print("="*70)

    # Cache frequently accessed data
    print("\n🔧 Caching frequently accessed partition...")

    hot_data = spark.sql("""
        SELECT *
        FROM lakehouse.perf_test
        WHERE event_date = DATE '2024-01-01'
    """)

    hot_data.cache()
    count1 = hot_data.count()  # Materialize cache

    print(f"\n   Cached {count1} records")
    print("   Subsequent queries will use cached data")

    # Query cached data
    result = hot_data.filter(col("value") > 50).count()
    print(f"   Filtered cached data: {result} records")

    # Unpersist when done
    hot_data.unpersist()

    print("\n✅ Caching Best Practices:")
    print("   ✅ Cache hot partitions")
    print("   ✅ Use for iterative queries")
    print("   ✅ Monitor memory usage")
    print("   ✅ Unpersist when done")


def example_maintenance_schedule(spark):
    """Example 10: Recommended maintenance schedule."""

    print("\n" + "="*70)
    print("EXAMPLE 10: MAINTENANCE SCHEDULE")
    print("="*70)

    print("\n📅 RECOMMENDED MAINTENANCE SCHEDULE:")

    print("\n🔄 DAILY:")
    print("   • Monitor file sizes")
    print("   • Check partition skew")
    print("   • Review query performance")

    print("\n🔄 WEEKLY:")
    print("   • Compact small files")
    print("   • Expire old snapshots (retain 7-14 days)")
    print("   • Analyze partition statistics")

    print("\n🔄 MONTHLY:")
    print("   • Remove orphan files")
    print("   • Full table compaction")
    print("   • Review table properties")
    print("   • Optimize partition strategy")

    print("\n📊 MONITORING QUERIES:")

    print("\n   Check file counts by partition:")
    print("""
        SELECT partition, COUNT(*) as file_count
        FROM table.files
        GROUP BY partition
        ORDER BY file_count DESC
    """)

    print("\n   Check small files:")
    print("""
        SELECT COUNT(*) as small_file_count
        FROM table.files
        WHERE file_size_in_bytes < 10485760  -- < 10 MB
    """)

    print("\n   Check snapshot age:")
    print("""
        SELECT
            snapshot_id,
            committed_at,
            DATEDIFF(CURRENT_DATE, DATE(committed_at)) as age_days
        FROM table.snapshots
        ORDER BY committed_at DESC
    """)


def main():
    """Main execution function."""

    # Create Spark session
    spark = create_iceberg_spark_session(
        app_name="Iceberg Performance Optimization"
    )

    try:
        # Setup
        record_count = setup_performance_test_table(spark)
        print(f"\n✅ Test table created with {record_count} records")

        # Performance examples
        example_small_files_problem(spark)
        example_compaction(spark)
        example_partition_level_compaction(spark)
        example_snapshot_expiration(spark)
        example_orphan_file_removal(spark)
        example_metadata_cleanup(spark)
        example_query_optimization(spark)
        example_table_properties_tuning(spark)
        example_caching_strategies(spark)
        example_maintenance_schedule(spark)

        print("\n" + "="*70)
        print("✅ ALL PERFORMANCE OPTIMIZATION EXAMPLES COMPLETED!")
        print("="*70)

        print("\n📚 KEY OPTIMIZATION TECHNIQUES:")
        print("   ✅ File compaction - Reduce small files")
        print("   ✅ Snapshot expiration - Clean old history")
        print("   ✅ Orphan file removal - Reclaim storage")
        print("   ✅ Partition pruning - Skip irrelevant data")
        print("   ✅ Table properties tuning - Optimize settings")
        print("   ✅ Caching strategies - Speed up queries")
        print("   ✅ Regular maintenance - Keep tables healthy")

        print("\n⚡ PERFORMANCE BEST PRACTICES:")
        print("   • Target 128 MB - 1 GB file sizes")
        print("   • Compact weekly or when files > 100")
        print("   • Expire snapshots after 7-14 days")
        print("   • Remove orphans monthly")
        print("   • Use ZSTD compression")
        print("   • Enable vectorization")
        print("   • Monitor with metadata queries")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
