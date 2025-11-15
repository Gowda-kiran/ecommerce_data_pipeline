"""
Apache Iceberg Schema Evolution Examples
Demonstrates schema changes without data rewrite - a key Iceberg feature
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from config.spark_iceberg_config import create_iceberg_spark_session


def setup_initial_table(spark):
    """Create initial table with basic schema."""

    print("\n" + "="*70)
    print("SETUP: Creating Initial Table")
    print("="*70)

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS lakehouse.products")

    # Create initial table with basic schema
    spark.sql("""
        CREATE TABLE lakehouse.products (
            product_id STRING,
            product_name STRING,
            category STRING,
            price DOUBLE,
            in_stock BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (category)
    """)

    # Insert initial data
    spark.sql("""
        INSERT INTO lakehouse.products VALUES
        ('P001', 'Laptop', 'Electronics', 999.99, true),
        ('P002', 'Mouse', 'Electronics', 29.99, true),
        ('P003', 'Desk', 'Furniture', 299.99, true),
        ('P004', 'Chair', 'Furniture', 199.99, false),
        ('P005', 'Monitor', 'Electronics', 399.99, true)
    """)

    print("\n📊 Initial Schema:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n📊 Initial Data:")
    spark.sql("SELECT * FROM lakehouse.products ORDER BY product_id").show(truncate=False)

    return spark.sql("SELECT COUNT(*) as count FROM lakehouse.products").collect()[0].count


def example_add_columns(spark):
    """Example 1: Add new columns to the schema."""

    print("\n" + "="*70)
    print("EXAMPLE 1: ADD COLUMNS")
    print("="*70)

    print("\n🔧 Adding 'brand' column...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        ADD COLUMN brand STRING
    """)

    print("\n🔧 Adding 'weight_kg' column with comment...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        ADD COLUMN weight_kg DOUBLE COMMENT 'Product weight in kilograms'
    """)

    print("\n🔧 Adding 'warranty_months' column...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        ADD COLUMN warranty_months INT
    """)

    print("\n📊 Updated Schema:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n📊 Existing Data (new columns are NULL):")
    spark.sql("SELECT * FROM lakehouse.products ORDER BY product_id").show(truncate=False)

    # Insert new data with new columns
    print("\n✅ Inserting new data with new columns...")
    spark.sql("""
        INSERT INTO lakehouse.products VALUES
        ('P006', 'Keyboard', 'Electronics', 79.99, true, 'Logitech', 0.8, 24)
    """)

    print("\n📊 Data After Insert:")
    spark.sql("SELECT * FROM lakehouse.products ORDER BY product_id").show(truncate=False)

    # Update existing records with new column values
    print("\n✅ Updating existing records with new column values...")
    spark.sql("""
        UPDATE lakehouse.products
        SET brand = 'Dell', weight_kg = 2.5, warranty_months = 12
        WHERE product_id = 'P001'
    """)

    spark.sql("""
        UPDATE lakehouse.products
        SET brand = 'Logitech', weight_kg = 0.1, warranty_months = 6
        WHERE product_id = 'P002'
    """)

    print("\n📊 Data After Updates:")
    spark.sql("SELECT * FROM lakehouse.products ORDER BY product_id").show(truncate=False)


def example_rename_columns(spark):
    """Example 2: Rename columns."""

    print("\n" + "="*70)
    print("EXAMPLE 2: RENAME COLUMNS")
    print("="*70)

    print("\n🔧 Renaming 'product_name' to 'name'...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        RENAME COLUMN product_name TO name
    """)

    print("\n🔧 Renaming 'in_stock' to 'is_available'...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        RENAME COLUMN in_stock TO is_available
    """)

    print("\n📊 Updated Schema:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n📊 Data with Renamed Columns:")
    spark.sql("SELECT product_id, name, category, price, is_available FROM lakehouse.products ORDER BY product_id").show(truncate=False)

    print("\n✅ Old column names no longer work (this would error):")
    print("   SELECT product_name FROM lakehouse.products  -- Would fail!")
    print("\n✅ Must use new column names:")
    print("   SELECT name FROM lakehouse.products  -- Works!")


def example_update_column_comments(spark):
    """Example 3: Update column comments and metadata."""

    print("\n" + "="*70)
    print("EXAMPLE 3: UPDATE COLUMN METADATA")
    print("="*70)

    print("\n🔧 Adding comment to 'price' column...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        ALTER COLUMN price COMMENT 'Product price in USD'
    """)

    print("\n🔧 Adding comment to 'brand' column...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        ALTER COLUMN brand COMMENT 'Manufacturer brand name'
    """)

    print("\n📊 Schema with Comments:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n📊 Extended Schema Information:")
    spark.sql("DESCRIBE EXTENDED lakehouse.products").show(truncate=False)


def example_drop_columns(spark):
    """Example 4: Drop columns from schema."""

    print("\n" + "="*70)
    print("EXAMPLE 4: DROP COLUMNS")
    print("="*70)

    print("\n📊 Schema Before Dropping:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n🔧 Dropping 'warranty_months' column...")
    spark.sql("""
        ALTER TABLE lakehouse.products
        DROP COLUMN warranty_months
    """)

    print("\n📊 Schema After Dropping:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n📊 Data (dropped column no longer accessible):")
    spark.sql("SELECT * FROM lakehouse.products ORDER BY product_id").show(truncate=False)

    print("\n✅ Note: Dropping a column doesn't delete the data from files!")
    print("   The data still exists in the Parquet files, but is not accessible.")
    print("   This allows for instant column drops without data rewrite.")


def example_reorder_columns(spark):
    """Example 5: Change column order."""

    print("\n" + "="*70)
    print("EXAMPLE 5: REORDER COLUMNS")
    print("="*70)

    print("\n📊 Current Column Order:")
    current_cols = [field.name for field in spark.table("lakehouse.products").schema.fields]
    print("   ", " -> ".join(current_cols))

    # Note: Iceberg doesn't have direct column reordering
    # But we can work around it by selecting in desired order
    print("\n✅ To change column display order, use SELECT with specific order:")

    desired_order = spark.sql("""
        SELECT
            product_id,
            category,
            name,
            brand,
            price,
            weight_kg,
            is_available
        FROM lakehouse.products
        ORDER BY product_id
    """)

    print("\n📊 Data with Desired Column Order:")
    desired_order.show(truncate=False)

    print("\n💡 Tip: For permanent reordering, create a new table or use CREATE OR REPLACE")


def example_type_evolution(spark):
    """Example 6: Demonstrate type compatibility and evolution."""

    print("\n" + "="*70)
    print("EXAMPLE 6: TYPE EVOLUTION")
    print("="*70)

    # Create a new table for type evolution example
    spark.sql("DROP TABLE IF EXISTS lakehouse.type_evolution_demo")

    spark.sql("""
        CREATE TABLE lakehouse.type_evolution_demo (
            id STRING,
            quantity INT,
            measurement FLOAT,
            metadata STRING
        )
        USING iceberg
    """)

    # Insert initial data
    spark.sql("""
        INSERT INTO lakehouse.type_evolution_demo VALUES
        ('A001', 100, 10.5, 'initial'),
        ('A002', 200, 20.7, 'initial')
    """)

    print("\n📊 Initial Schema and Data:")
    spark.sql("DESCRIBE lakehouse.type_evolution_demo").show(truncate=False)
    spark.sql("SELECT * FROM lakehouse.type_evolution_demo").show(truncate=False)

    print("\n🔧 Type Evolution - Widening Types (Safe Operations):")
    print("\n   ✅ INT → LONG (widening)")
    spark.sql("""
        ALTER TABLE lakehouse.type_evolution_demo
        ALTER COLUMN quantity TYPE BIGINT
    """)

    print("\n   ✅ FLOAT → DOUBLE (widening)")
    spark.sql("""
        ALTER TABLE lakehouse.type_evolution_demo
        ALTER COLUMN measurement TYPE DOUBLE
    """)

    print("\n📊 Updated Schema After Type Widening:")
    spark.sql("DESCRIBE lakehouse.type_evolution_demo").show(truncate=False)

    print("\n📊 Existing Data (no issues):")
    spark.sql("SELECT * FROM lakehouse.type_evolution_demo").show(truncate=False)

    # Insert new data with wider types
    print("\n✅ Inserting data with larger values (now possible):")
    spark.sql("""
        INSERT INTO lakehouse.type_evolution_demo VALUES
        ('A003', 9999999999, 12345.6789012, 'large_values')
    """)

    print("\n📊 Data After Insert with Large Values:")
    spark.sql("SELECT * FROM lakehouse.type_evolution_demo").show(truncate=False)

    print("\n⚠️  Type Evolution Restrictions:")
    print("   ❌ Cannot narrow types (e.g., LONG → INT)")
    print("   ❌ Cannot convert incompatible types (e.g., STRING → INT)")
    print("   ✅ Can widen numeric types (INT → LONG, FLOAT → DOUBLE)")
    print("   ✅ Can promote types (e.g., DECIMAL precision increase)")


def example_nested_schema_evolution(spark):
    """Example 7: Schema evolution with nested structures."""

    print("\n" + "="*70)
    print("EXAMPLE 7: NESTED SCHEMA EVOLUTION")
    print("="*70)

    # Create table with nested structure
    spark.sql("DROP TABLE IF EXISTS lakehouse.orders")

    spark.sql("""
        CREATE TABLE lakehouse.orders (
            order_id STRING,
            customer STRUCT<
                id: STRING,
                name: STRING
            >,
            order_date DATE,
            total_amount DOUBLE
        )
        USING iceberg
    """)

    # Insert initial data
    spark.sql("""
        INSERT INTO lakehouse.orders VALUES
        ('O001', STRUCT('C001', 'John Doe'), DATE '2024-01-15', 150.00),
        ('O002', STRUCT('C002', 'Jane Smith'), DATE '2024-01-16', 275.50)
    """)

    print("\n📊 Initial Nested Schema:")
    spark.sql("DESCRIBE lakehouse.orders").show(truncate=False)

    print("\n📊 Initial Data:")
    spark.table("lakehouse.orders").select("order_id", "customer.*", "order_date", "total_amount").show(truncate=False)

    print("\n🔧 Adding field to nested structure...")
    spark.sql("""
        ALTER TABLE lakehouse.orders
        ADD COLUMN customer.email STRING COMMENT 'Customer email address'
    """)

    print("\n🔧 Adding another field to nested structure...")
    spark.sql("""
        ALTER TABLE lakehouse.orders
        ADD COLUMN customer.phone STRING
    """)

    print("\n📊 Updated Nested Schema:")
    spark.sql("DESCRIBE lakehouse.orders").show(truncate=False)

    print("\n📊 Existing Data (new nested fields are NULL):")
    spark.table("lakehouse.orders").select(
        "order_id",
        "customer.id",
        "customer.name",
        "customer.email",
        "customer.phone",
        "order_date",
        "total_amount"
    ).show(truncate=False)

    # Insert new data with nested fields
    print("\n✅ Inserting new data with nested fields...")
    spark.sql("""
        INSERT INTO lakehouse.orders VALUES
        ('O003', STRUCT('C003', 'Bob Wilson', 'bob@example.com', '555-0123'), DATE '2024-01-17', 320.00)
    """)

    print("\n📊 Data After Insert:")
    spark.table("lakehouse.orders").select(
        "order_id",
        "customer.id",
        "customer.name",
        "customer.email",
        "customer.phone",
        "order_date",
        "total_amount"
    ).show(truncate=False)


def example_schema_history(spark):
    """Example 8: View schema evolution history."""

    print("\n" + "="*70)
    print("EXAMPLE 8: SCHEMA EVOLUTION HISTORY")
    print("="*70)

    print("\n📋 Table Snapshots (showing schema changes over time):")
    spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            summary
        FROM lakehouse.products.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    print("\n📋 Table History:")
    spark.sql("""
        SELECT
            made_current_at,
            snapshot_id,
            is_current_ancestor
        FROM lakehouse.products.history
        ORDER BY made_current_at DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n📋 Current Table Metadata:")
    spark.sql("""
        SELECT
            key,
            value
        FROM lakehouse.products.refs
    """).show(truncate=False)


def example_backward_compatibility(spark):
    """Example 9: Demonstrate backward compatibility with schema evolution."""

    print("\n" + "="*70)
    print("EXAMPLE 9: BACKWARD COMPATIBILITY")
    print("="*70)

    print("\n✅ Reading old snapshots with old schema:")

    # Get first snapshot
    first_snapshot = spark.sql("""
        SELECT snapshot_id
        FROM lakehouse.products.snapshots
        ORDER BY committed_at
        LIMIT 1
    """).collect()

    if first_snapshot:
        snapshot_id = first_snapshot[0].snapshot_id
        print(f"\n📊 Reading snapshot {snapshot_id} (original schema):")

        old_data = spark.read \
            .option("snapshot-id", snapshot_id) \
            .table("lakehouse.products")

        print("\nOriginal Schema Columns:", old_data.columns)
        old_data.show(truncate=False)

        print("\n✅ Current table has evolved schema:")
        current_data = spark.table("lakehouse.products")
        print("Current Schema Columns:", current_data.columns)
        current_data.show(truncate=False)

        print("\n💡 Key Points:")
        print("   ✅ Old snapshots remain readable with their original schema")
        print("   ✅ New columns don't affect old data queries")
        print("   ✅ Schema evolution is non-breaking for existing queries")
        print("   ✅ Time travel works seamlessly across schema versions")


def example_schema_validation(spark):
    """Example 10: Schema validation and constraints."""

    print("\n" + "="*70)
    print("EXAMPLE 10: SCHEMA VALIDATION")
    print("="*70)

    print("\n📊 Current Schema:")
    spark.sql("DESCRIBE lakehouse.products").show(truncate=False)

    print("\n✅ Iceberg automatically validates:")
    print("   • Data types match schema")
    print("   • Required fields are not NULL (if specified)")
    print("   • Column names match")
    print("   • Partition spec compliance")

    print("\n💡 Example of type validation:")
    print("   Trying to insert STRING into DOUBLE column will fail")
    print("   Iceberg validates at write time, ensuring data quality")

    # Show table properties
    print("\n📋 Table Properties:")
    spark.sql("SHOW TBLPROPERTIES lakehouse.products").show(truncate=False)


def main():
    """Main execution function."""

    # Create Spark session
    spark = create_iceberg_spark_session(
        app_name="Iceberg Schema Evolution Examples"
    )

    try:
        # Setup initial table
        initial_count = setup_initial_table(spark)
        print(f"\n✅ Initial table created with {initial_count} records")

        # Example 1: Add columns
        example_add_columns(spark)

        # Example 2: Rename columns
        example_rename_columns(spark)

        # Example 3: Update column metadata
        example_update_column_comments(spark)

        # Example 4: Drop columns
        example_drop_columns(spark)

        # Example 5: Reorder columns
        example_reorder_columns(spark)

        # Example 6: Type evolution
        example_type_evolution(spark)

        # Example 7: Nested schema evolution
        example_nested_schema_evolution(spark)

        # Example 8: Schema history
        example_schema_history(spark)

        # Example 9: Backward compatibility
        example_backward_compatibility(spark)

        # Example 10: Schema validation
        example_schema_validation(spark)

        print("\n" + "="*70)
        print("✅ ALL SCHEMA EVOLUTION EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*70)

        print("\n📚 KEY TAKEAWAYS:")
        print("   ✅ Add columns without data rewrite")
        print("   ✅ Rename columns safely")
        print("   ✅ Drop columns instantly")
        print("   ✅ Evolve types (widening)")
        print("   ✅ Modify nested structures")
        print("   ✅ Full backward compatibility")
        print("   ✅ Schema versioning with time travel")
        print("   ✅ No downtime for schema changes")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
