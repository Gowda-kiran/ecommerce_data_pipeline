"""
Spark Configuration for 500GB Big Data Pipeline
Optimized settings for production workloads
"""

# Spark configurations optimized for 500GB daily processing
SPARK_CONFIGS = {
    # Application
    "spark.app.name": "Ecommerce-500GB-Pipeline",

    # Adaptive Query Execution (AQE)
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "400",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",

    # Shuffle
    "spark.sql.shuffle.partitions": "400",
    "spark.shuffle.service.enabled": "true",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",

    # Memory
    "spark.executor.memory": "8g",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "4g",
    "spark.driver.memoryOverhead": "1g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",

    # Cores
    "spark.executor.cores": "4",
    "spark.driver.cores": "2",
    "spark.default.parallelism": "400",

    # Broadcast
    "spark.sql.autoBroadcastJoinThreshold": "104857600",  # 100MB
    "spark.broadcast.blockSize": "8m",

    # File Operations
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
    "spark.sql.files.minPartitionNum": "1",
    "spark.sql.files.maxRecordsPerFile": "0",  # Unlimited

    # Compression
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.orc.compression.codec": "snappy",

    # Optimization
    "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
    "spark.sql.cbo.enabled": "true",
    "spark.sql.statistics.histogram.enabled": "true",

    # I/O
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",

    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrationRequired": "false",

    # Speculation
    "spark.speculation": "true",
    "spark.speculation.multiplier": "1.5",

    # Dynamic Allocation
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "10",
    "spark.dynamicAllocation.maxExecutors": "200",
    "spark.dynamicAllocation.initialExecutors": "50",

    # Iceberg specific
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "s3://data-lake/iceberg_warehouse",

    # Logging
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3://spark-logs/",
}


# Environment-specific overrides
DEV_OVERRIDES = {
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.dynamicAllocation.maxExecutors": "20",
}

PROD_OVERRIDES = {
    "spark.executor.memory": "16g",
    "spark.executor.cores": "8",
    "spark.dynamicAllocation.maxExecutors": "500",
}


def get_spark_config(environment="dev"):
    """Get Spark configuration for specified environment."""
    config = SPARK_CONFIGS.copy()

    if environment == "dev":
        config.update(DEV_OVERRIDES)
    elif environment == "prod":
        config.update(PROD_OVERRIDES)

    return config


def apply_spark_config(spark_builder, environment="dev"):
    """Apply configuration to Spark session builder."""
    config = get_spark_config(environment)

    for key, value in config.items():
        spark_builder = spark_builder.config(key, value)

    return spark_builder
