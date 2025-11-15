"""
Apache Iceberg CDC and ACID Transactions Examples
Demonstrates Change Data Capture patterns and ACID guarantees
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, row_number
from pyspark.sql.window import Window
from datetime import datetime
from config.spark_iceberg_config import create_iceberg_spark_session


def setup_cdc_tables(spark):
    """Setup tables for CDC demonstration."""

    print("\n" + "="*70)
    print("SETUP: Creating CDC Tables")
    print("="*70)

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS lakehouse.customers")
    spark.sql("DROP TABLE IF EXISTS lakehouse.cdc_changelog")

    # Create main customers table
    spark.sql("""
        CREATE TABLE lakehouse.customers (
            customer_id STRING,
            name STRING,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            country STRING,
            status STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (country)
    """)

    # Create CDC changelog table
    spark.sql("""
        CREATE TABLE lakehouse.cdc_changelog (
            change_id BIGINT,
            customer_id STRING,
            change_type STRING,
            change_timestamp TIMESTAMP,
            before_image MAP<STRING, STRING>,
            after_image MAP<STRING, STRING>,
            changed_fields ARRAY<STRING>
        )
        USING iceberg
        PARTITIONED BY (days(change_timestamp))
    """)

    print("\n✅ CDC tables created")

    # Insert initial customer data
    print("\n📝 Inserting initial customer data...")
    spark.sql("""
        INSERT INTO lakehouse.customers VALUES
        ('C001', 'John Doe', 'john@example.com', '555-0001', '123 Main St', 'New York', 'US', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('C002', 'Jane Smith', 'jane@example.com', '555-0002', '456 Oak Ave', 'London', 'UK', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('C003', 'Bob Wilson', 'bob@example.com', '555-0003', '789 Pine Rd', 'Toronto', 'CA', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('C004', 'Alice Brown', 'alice@example.com', '555-0004', '321 Elm St', 'Sydney', 'AU', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('C005', 'Charlie Davis', 'charlie@example.com', '555-0005', '654 Maple Dr', 'Berlin', 'DE', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    print("\n📊 Initial Customer Data:")
    spark.sql("SELECT * FROM lakehouse.customers ORDER BY customer_id").show(truncate=False)

    return 5


def example_acid_atomicity(spark):
    """Example 1: Demonstrate ACID Atomicity."""

    print("\n" + "="*70)
    print("EXAMPLE 1: ACID ATOMICITY")
    print("="*70)

    print("\n💡 Atomicity: All operations in a transaction succeed or all fail")

    # Get initial count
    initial_count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.customers").collect()[0].count
    print(f"\n📊 Initial customer count: {initial_count}")

    # Successful transaction (all or nothing)
    print("\n🔧 Executing transaction with multiple inserts...")

    try:
        # In Iceberg, each write is automatically atomic
        spark.sql("""
            INSERT INTO lakehouse.customers VALUES
            ('C006', 'David Green', 'david@example.com', '555-0006', '111 First St', 'Paris', 'FR', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('C007', 'Emma White', 'emma@example.com', '555-0007', '222 Second St', 'Madrid', 'ES', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)

        final_count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.customers").collect()[0].count
        print(f"\n✅ Transaction succeeded!")
        print(f"   Before: {initial_count} customers")
        print(f"   After: {final_count} customers")
        print(f"   Added: {final_count - initial_count} customers")

    except Exception as e:
        print(f"\n❌ Transaction failed: {e}")
        print("   All changes rolled back automatically")

    print("\n💡 Key Point: In Iceberg, writes are atomic at the snapshot level")
    print("   Either the entire write succeeds and creates a new snapshot,")
    print("   or it fails and no snapshot is created.")


def example_acid_consistency(spark):
    """Example 2: Demonstrate ACID Consistency."""

    print("\n" + "="*70)
    print("EXAMPLE 2: ACID CONSISTENCY")
    print("="*70)

    print("\n💡 Consistency: Data remains in a valid state before and after transactions")

    # Show schema constraints
    print("\n📋 Table Schema with Constraints:")
    spark.sql("DESCRIBE EXTENDED lakehouse.customers").show(truncate=False)

    print("\n🔧 Attempting to insert valid data...")
    spark.sql("""
        INSERT INTO lakehouse.customers VALUES
        ('C008', 'Frank Black', 'frank@example.com', '555-0008', '333 Third St', 'Rome', 'IT', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)
    print("✅ Valid data inserted successfully")

    print("\n💡 Iceberg ensures:")
    print("   ✅ Schema validation on every write")
    print("   ✅ Data type consistency")
    print("   ✅ Partition spec compliance")
    print("   ✅ Table constraints enforcement")


def example_acid_isolation(spark):
    """Example 3: Demonstrate ACID Isolation."""

    print("\n" + "="*70)
    print("EXAMPLE 3: ACID ISOLATION")
    print("="*70)

    print("\n💡 Isolation: Concurrent transactions don't interfere with each other")

    # Simulate concurrent reads at different snapshot levels
    print("\n📊 Snapshot Isolation:")

    # Get current snapshot
    current_snapshot = spark.sql("""
        SELECT snapshot_id, committed_at
        FROM lakehouse.customers.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """).collect()[0]

    print(f"\n   Current snapshot: {current_snapshot.snapshot_id}")
    print(f"   Committed at: {current_snapshot.committed_at}")

    # Read at current snapshot
    current_data = spark.table("lakehouse.customers")
    current_count = current_data.count()
    print(f"\n   Records at current snapshot: {current_count}")

    # Perform a write (creates new snapshot)
    print("\n🔧 Writing new data (creates new snapshot)...")
    spark.sql("""
        INSERT INTO lakehouse.customers VALUES
        ('C009', 'Grace Lee', 'grace@example.com', '555-0009', '444 Fourth St', 'Tokyo', 'JP', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    # Read new snapshot
    new_data = spark.table("lakehouse.customers")
    new_count = new_data.count()
    print(f"   Records at new snapshot: {new_count}")

    # Read old snapshot (time travel)
    print(f"\n📊 Reading old snapshot {current_snapshot.snapshot_id}...")
    old_snapshot_data = spark.read \
        .option("snapshot-id", current_snapshot.snapshot_id) \
        .table("lakehouse.customers")
    old_count = old_snapshot_data.count()
    print(f"   Records at old snapshot: {old_count}")

    print("\n✅ Snapshot Isolation Benefits:")
    print(f"   • Old readers see {old_count} records (snapshot {current_snapshot.snapshot_id})")
    print(f"   • New readers see {new_count} records (latest snapshot)")
    print("   • No interference between concurrent operations")
    print("   • Readers never block writers, writers never block readers")


def example_acid_durability(spark):
    """Example 4: Demonstrate ACID Durability."""

    print("\n" + "="*70)
    print("EXAMPLE 4: ACID DURABILITY")
    print("="*70)

    print("\n💡 Durability: Committed data persists even after failures")

    # Show snapshots (persistent commits)
    print("\n📋 Committed Snapshots (durable records):")
    spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            summary
        FROM lakehouse.customers.snapshots
        ORDER BY committed_at DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n📁 Data Files (persisted to storage):")
    spark.sql("""
        SELECT
            file_path,
            file_format,
            record_count,
            file_size_in_bytes / 1024 as size_kb
        FROM lakehouse.customers.files
        LIMIT 5
    """).show(truncate=False)

    print("\n✅ Durability Guarantees:")
    print("   • Each commit creates immutable snapshot metadata")
    print("   • Data files written to durable storage (HDFS, S3, etc.)")
    print("   • Metadata operations are atomic file system operations")
    print("   • Can recover from any committed snapshot")


def example_cdc_insert_pattern(spark):
    """Example 5: CDC pattern for INSERT operations."""

    print("\n" + "="*70)
    print("EXAMPLE 5: CDC - INSERT PATTERN")
    print("="*70)

    print("\n🔧 Inserting new customer and logging to CDC...")

    # Insert new customer
    spark.sql("""
        INSERT INTO lakehouse.customers VALUES
        ('C010', 'Henry Ford', 'henry@example.com', '555-0010', '555 Fifth Ave', 'Chicago', 'US', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    # Log CDC event
    spark.sql("""
        INSERT INTO lakehouse.cdc_changelog VALUES
        (1, 'C010', 'INSERT', CURRENT_TIMESTAMP, MAP(), MAP('name', 'Henry Ford', 'email', 'henry@example.com', 'status', 'active'), ARRAY('name', 'email', 'status'))
    """)

    print("\n📊 CDC Changelog:")
    spark.sql("SELECT * FROM lakehouse.cdc_changelog ORDER BY change_id").show(truncate=False)

    print("\n✅ INSERT CDC Pattern:")
    print("   • change_type = 'INSERT'")
    print("   • before_image = empty")
    print("   • after_image = new record")


def example_cdc_update_pattern(spark):
    """Example 6: CDC pattern for UPDATE operations."""

    print("\n" + "="*70)
    print("EXAMPLE 6: CDC - UPDATE PATTERN")
    print("="*70)

    # Get current state
    before = spark.sql("""
        SELECT customer_id, email, status
        FROM lakehouse.customers
        WHERE customer_id = 'C001'
    """).collect()[0]

    print(f"\n📊 Before Update:")
    print(f"   Customer: {before.customer_id}")
    print(f"   Email: {before.email}")
    print(f"   Status: {before.status}")

    # Update customer
    print("\n🔧 Updating customer email and status...")
    spark.sql("""
        UPDATE lakehouse.customers
        SET email = 'john.doe@newemail.com',
            status = 'premium',
            updated_at = CURRENT_TIMESTAMP
        WHERE customer_id = 'C001'
    """)

    # Log CDC event
    spark.sql("""
        INSERT INTO lakehouse.cdc_changelog VALUES
        (
            2,
            'C001',
            'UPDATE',
            CURRENT_TIMESTAMP,
            MAP('email', 'john@example.com', 'status', 'active'),
            MAP('email', 'john.doe@newemail.com', 'status', 'premium'),
            ARRAY('email', 'status')
        )
    """)

    # Show after state
    after = spark.sql("""
        SELECT customer_id, email, status
        FROM lakehouse.customers
        WHERE customer_id = 'C001'
    """).collect()[0]

    print(f"\n📊 After Update:")
    print(f"   Customer: {after.customer_id}")
    print(f"   Email: {after.email}")
    print(f"   Status: {after.status}")

    print("\n📊 CDC Changelog:")
    spark.sql("SELECT * FROM lakehouse.cdc_changelog WHERE customer_id = 'C001'").show(truncate=False)

    print("\n✅ UPDATE CDC Pattern:")
    print("   • change_type = 'UPDATE'")
    print("   • before_image = old values")
    print("   • after_image = new values")
    print("   • changed_fields = list of modified columns")


def example_cdc_delete_pattern(spark):
    """Example 7: CDC pattern for DELETE operations."""

    print("\n" + "="*70)
    print("EXAMPLE 7: CDC - DELETE PATTERN")
    print("="*70)

    # Get current state
    before = spark.sql("""
        SELECT *
        FROM lakehouse.customers
        WHERE customer_id = 'C005'
    """)

    print("\n📊 Before Delete:")
    before.show(truncate=False)

    # Delete customer
    print("\n🔧 Deleting customer C005...")
    spark.sql("""
        DELETE FROM lakehouse.customers
        WHERE customer_id = 'C005'
    """)

    # Log CDC event (capturing deleted record)
    spark.sql("""
        INSERT INTO lakehouse.cdc_changelog VALUES
        (
            3,
            'C005',
            'DELETE',
            CURRENT_TIMESTAMP,
            MAP('name', 'Charlie Davis', 'email', 'charlie@example.com', 'status', 'active'),
            MAP(),
            ARRAY('name', 'email', 'status', 'phone', 'address', 'city', 'country')
        )
    """)

    # Verify deletion
    after = spark.sql("""
        SELECT COUNT(*) as count
        FROM lakehouse.customers
        WHERE customer_id = 'C005'
    """).collect()[0].count

    print(f"\n📊 After Delete: {after} records found (deleted)")

    print("\n📊 CDC Changelog:")
    spark.sql("SELECT * FROM lakehouse.cdc_changelog WHERE customer_id = 'C005'").show(truncate=False)

    print("\n✅ DELETE CDC Pattern:")
    print("   • change_type = 'DELETE'")
    print("   • before_image = deleted record")
    print("   • after_image = empty")


def example_cdc_merge_upsert(spark):
    """Example 8: CDC with MERGE (UPSERT) operations."""

    print("\n" + "="*70)
    print("EXAMPLE 8: CDC - MERGE (UPSERT) PATTERN")
    print("="*70)

    # Create staging data
    print("\n🔧 Creating staging data for merge...")
    staging_data = [
        ('C002', 'Jane Smith Updated', 'jane.smith@newemail.com', '555-0002', '456 Oak Ave', 'London', 'UK', 'premium'),  # UPDATE
        ('C011', 'Irene Carter', 'irene@example.com', '555-0011', '666 Sixth St', 'Dublin', 'IE', 'active'),  # INSERT
        ('C003', 'Bob Wilson Jr', 'bob.jr@example.com', '555-0003', '789 Pine Rd', 'Toronto', 'CA', 'active'),  # UPDATE
    ]

    staging_df = spark.createDataFrame(staging_data,
        ['customer_id', 'name', 'email', 'phone', 'address', 'city', 'country', 'status'])

    staging_df.createOrReplaceTempView("staging_customers")

    print("\n📊 Staging Data:")
    staging_df.show(truncate=False)

    print("\n🔧 Executing MERGE operation...")

    spark.sql("""
        MERGE INTO lakehouse.customers t
        USING staging_customers s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                t.name = s.name,
                t.email = s.email,
                t.phone = s.phone,
                t.status = s.status,
                t.updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, phone, address, city, country, status, created_at, updated_at)
            VALUES (s.customer_id, s.name, s.email, s.phone, s.address, s.city, s.country, s.status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    print("\n✅ MERGE completed!")

    # Show updated customers
    print("\n📊 Updated Customers:")
    spark.sql("""
        SELECT customer_id, name, email, status
        FROM lakehouse.customers
        WHERE customer_id IN ('C002', 'C003', 'C011')
        ORDER BY customer_id
    """).show(truncate=False)

    print("\n💡 MERGE Benefits:")
    print("   ✅ Single operation for INSERT + UPDATE")
    print("   ✅ Atomic UPSERT semantics")
    print("   ✅ Efficient CDC processing")
    print("   ✅ Handles late-arriving data")


def example_cdc_incremental_processing(spark):
    """Example 9: Incremental CDC processing."""

    print("\n" + "="*70)
    print("EXAMPLE 9: INCREMENTAL CDC PROCESSING")
    print("="*70)

    # Get snapshots for incremental read
    snapshots = spark.sql("""
        SELECT snapshot_id, committed_at
        FROM lakehouse.customers.snapshots
        ORDER BY committed_at
    """).collect()

    if len(snapshots) >= 2:
        start_snapshot = snapshots[-2].snapshot_id
        end_snapshot = snapshots[-1].snapshot_id

        print(f"\n🔧 Reading incremental changes...")
        print(f"   From snapshot: {start_snapshot}")
        print(f"   To snapshot: {end_snapshot}")

        # Read incremental changes
        incremental_df = spark.read \
            .format("iceberg") \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .load("lakehouse.customers")

        print("\n📊 Incremental Changes:")
        incremental_df.show(truncate=False)

        print(f"\n   Total changes: {incremental_df.count()}")

        print("\n✅ Incremental Processing Benefits:")
        print("   • Process only new/changed data")
        print("   • No need to scan entire table")
        print("   • Efficient for large tables")
        print("   • Exactly-once semantics with checkpointing")


def example_cdc_time_travel_recovery(spark):
    """Example 10: CDC with time travel for recovery."""

    print("\n" + "="*70)
    print("EXAMPLE 10: CDC TIME TRAVEL RECOVERY")
    print("="*70)

    # Show all snapshots
    print("\n📋 All Snapshots:")
    snapshots = spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            operation
        FROM lakehouse.customers.snapshots
        ORDER BY committed_at
    """)
    snapshots.show(truncate=False)

    snapshot_list = snapshots.collect()

    if len(snapshot_list) >= 2:
        # Get second snapshot
        recovery_snapshot = snapshot_list[1]

        print(f"\n🔧 Time Travel: Reading state at {recovery_snapshot.committed_at}")
        print(f"   Snapshot ID: {recovery_snapshot.snapshot_id}")

        historical_df = spark.read \
            .option("snapshot-id", recovery_snapshot.snapshot_id) \
            .table("lakehouse.customers")

        print("\n📊 Historical State:")
        historical_df.show(truncate=False)
        print(f"   Total records: {historical_df.count()}")

        print("\n📊 Current State:")
        current_df = spark.table("lakehouse.customers")
        current_df.show(truncate=False)
        print(f"   Total records: {current_df.count()}")

        print("\n✅ CDC Recovery Scenarios:")
        print("   • Audit changes between time points")
        print("   • Recover from accidental deletions")
        print("   • Compare states across time")
        print("   • Regulatory compliance & data lineage")


def main():
    """Main execution function."""

    # Create Spark session
    spark = create_iceberg_spark_session(
        app_name="Iceberg CDC and ACID Examples"
    )

    try:
        # Setup CDC tables
        record_count = setup_cdc_tables(spark)
        print(f"\n✅ CDC tables created with {record_count} initial records")

        # ACID Examples
        example_acid_atomicity(spark)
        example_acid_consistency(spark)
        example_acid_isolation(spark)
        example_acid_durability(spark)

        # CDC Pattern Examples
        example_cdc_insert_pattern(spark)
        example_cdc_update_pattern(spark)
        example_cdc_delete_pattern(spark)
        example_cdc_merge_upsert(spark)
        example_cdc_incremental_processing(spark)
        example_cdc_time_travel_recovery(spark)

        print("\n" + "="*70)
        print("✅ ALL CDC AND ACID EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*70)

        print("\n📚 KEY TAKEAWAYS:")
        print("\n   ACID Properties:")
        print("   ✅ Atomicity - Snapshot-level commits")
        print("   ✅ Consistency - Schema validation")
        print("   ✅ Isolation - Snapshot isolation")
        print("   ✅ Durability - Persistent snapshots")

        print("\n   CDC Patterns:")
        print("   ✅ INSERT - Track new records")
        print("   ✅ UPDATE - Capture field changes")
        print("   ✅ DELETE - Log deletions")
        print("   ✅ MERGE - Atomic upserts")
        print("   ✅ Incremental - Process changes only")
        print("   ✅ Time Travel - Recovery & audit")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
