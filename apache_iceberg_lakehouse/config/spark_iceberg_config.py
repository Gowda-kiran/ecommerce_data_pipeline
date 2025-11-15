"""
Spark Configuration for Apache Iceberg
Provides SparkSession with Iceberg integration
"""

from pyspark.sql import SparkSession
import os


def create_iceberg_spark_session(
    app_name="Iceberg Real-Time Lakehouse",
    warehouse_path="data/iceberg_warehouse",
    catalog_type="hadoop",
    enable_hive_support=True
):
    """
    Create SparkSession configured for Apache Iceberg.

    Args:
        app_name: Application name
        warehouse_path: Path to Iceberg warehouse
        catalog_type: Catalog type (hadoop, hive, glue, nessie)
        enable_hive_support: Enable Hive support

    Returns:
        SparkSession configured for Iceberg
    """

    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]")

    # Iceberg runtime dependency
    # Note: Version should match your Spark version
    builder = builder.config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2"
    )

    # Iceberg Spark extensions
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Configure catalog
    builder = builder.config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    builder = builder.config("spark.sql.catalog.iceberg.type", catalog_type)
    builder = builder.config("spark.sql.catalog.iceberg.warehouse", warehouse_path)

    # Default catalog and database
    builder = builder.config("spark.sql.defaultCatalog", "iceberg")

    # Additional Iceberg configurations
    builder = builder.config("spark.sql.catalog.iceberg.cache-enabled", "true")

    # Optimization configs
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Memory configurations
    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.executor.memory", "4g")

    # Shuffle partitions for better performance
    builder = builder.config("spark.sql.shuffle.partitions", "200")

    # Checkpointing for streaming
    checkpoint_dir = os.path.join(os.getcwd(), "data/checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    builder = builder.config("spark.sql.streaming.checkpointLocation", checkpoint_dir)

    # Enable Hive support if requested
    if enable_hive_support:
        builder = builder.enableHiveSupport()

    # Create session
    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Spark Session created successfully")
    print(f"📊 Spark Version: {spark.version}")
    print(f"🏷️  App Name: {app_name}")
    print(f"📁 Warehouse: {warehouse_path}")
    print(f"📚 Catalog Type: {catalog_type}")

    return spark


def create_glue_catalog_spark(
    app_name="Iceberg with AWS Glue",
    warehouse_path="s3://my-bucket/iceberg-warehouse",
    glue_database="iceberg_db",
    aws_region="us-east-1"
):
    """
    Create SparkSession with AWS Glue catalog for Iceberg.

    Args:
        app_name: Application name
        warehouse_path: S3 path for Iceberg warehouse
        glue_database: AWS Glue database name
        aws_region: AWS region

    Returns:
        SparkSession configured for Iceberg with Glue catalog
    """

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "software.amazon.awssdk:bundle:2.20.18") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.iceberg.warehouse", warehouse_path) \
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.glue.skip-name-validation", "true") \
        .config("spark.sql.catalog.iceberg.glue.region", aws_region) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Spark Session with Glue Catalog created")
    print(f"📁 Warehouse: {warehouse_path}")
    print(f"📚 Glue Database: {glue_database}")
    print(f"🌎 Region: {aws_region}")

    return spark


def show_iceberg_config(spark):
    """Display current Iceberg configuration."""

    print("\n" + "="*60)
    print("ICEBERG CONFIGURATION")
    print("="*60)

    configs = [
        "spark.sql.catalog.iceberg",
        "spark.sql.catalog.iceberg.type",
        "spark.sql.catalog.iceberg.warehouse",
        "spark.sql.extensions",
        "spark.sql.defaultCatalog"
    ]

    for config in configs:
        value = spark.conf.get(config, "Not Set")
        print(f"{config}: {value}")

    print("="*60 + "\n")


# Example usage
if __name__ == "__main__":
    # Create local Iceberg spark session
    spark = create_iceberg_spark_session(
        warehouse_path="data/iceberg_warehouse"
    )

    # Show configuration
    show_iceberg_config(spark)

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")

    # List databases
    print("Available databases:")
    spark.sql("SHOW DATABASES").show()

    # Stop session
    spark.stop()
