# PySpark Best Practices Guide

## 📋 Table of Contents
1. [Code Organization](#code-organization)
2. [Performance Best Practices](#performance-best-practices)
3. [Memory Management](#memory-management)
4. [Data Quality](#data-quality)
5. [Security](#security)
6. [Testing](#testing)
7. [Deployment](#deployment)

---

## 1. Code Organization

### ✅ DO: Use Meaningful Variable Names

```python
# ❌ BAD
df1 = spark.read.csv("data.csv")
df2 = df1.filter(...)
df3 = df2.groupBy(...)

# ✅ GOOD
customer_df = spark.read.csv("customers.csv")
active_customers = customer_df.filter(col("status") == "active")
customer_summary = active_customers.groupBy("region").count()
```

### ✅ DO: Break Complex Transformations into Steps

```python
# ❌ BAD - Hard to debug and understand
result = df.filter(...).groupBy(...).agg(...).join(...).filter(...).select(...)

# ✅ GOOD - Clear and debuggable
filtered_df = df.filter(col("date") >= "2024-01-01")
grouped_df = filtered_df.groupBy("customer_id").agg(sum("amount"))
enriched_df = grouped_df.join(customer_df, "customer_id")
final_df = enriched_df.filter(col("total") > 1000).select(required_columns)
```

### ✅ DO: Use Configuration Files

```python
# config.yaml
spark:
  app_name: "MyApp"
  executor_memory: "4g"
  driver_memory: "2g"

# Python code
import yaml

def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

config = load_config("config.yaml")
spark = SparkSession.builder \
    .appName(config['spark']['app_name']) \
    .config("spark.executor.memory", config['spark']['executor_memory']) \
    .getOrCreate()
```

### ✅ DO: Add Comments for Business Logic

```python
# ❌ BAD
df = df.filter(col("value") > 100)

# ✅ GOOD
# Filter out transactions below minimum threshold for fraud detection
# Business rule: Only analyze transactions > $100
df = df.filter(col("transaction_amount") > 100)
```

---

## 2. Performance Best Practices

### ✅ DO: Use DataFrame API Over RDD API

```python
# ❌ BAD - RDD (slower, no optimization)
rdd = sc.textFile("data.txt")
result = rdd.map(lambda x: x.split(",")) \
           .filter(lambda x: int(x[1]) > 100) \
           .collect()

# ✅ GOOD - DataFrame (faster, optimized by Catalyst)
df = spark.read.csv("data.csv")
result = df.filter(col("value") > 100).collect()
```

**Why?**
- Catalyst optimizer optimizes DataFrame operations
- Tungsten execution engine provides better memory management
- Column pruning and predicate pushdown

### ✅ DO: Use Built-in Functions Instead of UDFs

```python
from pyspark.sql.functions import upper, concat, lit

# ❌ BAD - UDF (serialization overhead)
@udf(StringType())
def uppercase_udf(s):
    return s.upper()

df = df.withColumn("name_upper", uppercase_udf("name"))

# ✅ GOOD - Built-in function (optimized)
df = df.withColumn("name_upper", upper("name"))
```

**Performance Impact:**
- Built-in: 8 seconds
- UDF: 45 seconds
- **Speed-up: 5.6x faster**

### ✅ DO: Filter Early, Select Late

```python
# ❌ BAD - Processing unnecessary columns
df = df.select("col1", "col2", "col3", ..., "col50")  # 50 columns
filtered_df = df.filter(col("status") == "active")
result = filtered_df.select("col1", "col2")  # Only need 2 columns

# ✅ GOOD - Filter first, then select needed columns
filtered_df = df.filter(col("status") == "active")
result = filtered_df.select("col1", "col2")  # Column pruning
```

**Why?**
- Column pruning reduces data to process
- Predicate pushdown filters at source
- Less data shuffled across network

### ✅ DO: Broadcast Small Tables in Joins

```python
from pyspark.sql.functions import broadcast

# Assume: large_df (10GB), small_df (10MB)

# ❌ BAD - Shuffle join
result = large_df.join(small_df, "key")

# ✅ GOOD - Broadcast join
result = large_df.join(broadcast(small_df), "key")
```

**When to use:**
- Small table < 10MB (default broadcast threshold)
- Joining large table with dimension table
- Reduces shuffle operations

**Configuration:**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB
```

### ✅ DO: Repartition for Optimal Parallelism

```python
# Check current partitions
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# ❌ BAD - Too many small partitions (overhead)
df = df.repartition(1000)  # 1000 partitions for 100MB data

# ❌ BAD - Too few partitions (underutilized)
df = df.repartition(2)  # 2 partitions on 10-node cluster

# ✅ GOOD - Optimal partitioning
# Rule of thumb: 2-4 partitions per core
num_cores = sc.defaultParallelism
df = df.repartition(num_cores * 2)
```

**Partition Size Guidelines:**
- Ideal partition size: 128MB - 256MB
- Formula: `num_partitions = total_data_size_MB / 128`

### ✅ DO: Use coalesce() Before Writing

```python
# ❌ BAD - Creates many small files
df.write.parquet("output")  # 1000 files

# ✅ GOOD - Controlled file count
df.coalesce(10).write.parquet("output")  # 10 files
```

**coalesce vs repartition:**
- `coalesce()`: Reduce partitions without shuffle (faster)
- `repartition()`: Can increase/decrease with shuffle

---

## 3. Memory Management

### ✅ DO: Cache Strategically

```python
# ✅ Cache when DataFrame is reused
df = spark.read.parquet("large_dataset")

# This DataFrame will be used multiple times
df_cached = df.filter(col("date") >= "2024-01-01").cache()

# Multiple actions on same DataFrame
df_cached.filter(col("status") == "active").count()
df_cached.groupBy("region").count().show()
df_cached.agg(sum("amount")).show()

# ❌ Don't forget to unpersist
df_cached.unpersist()
```

### ✅ DO: Choose Appropriate Storage Level

```python
from pyspark import StorageLevel

# For memory-rich environments
df.persist(StorageLevel.MEMORY_ONLY)

# For memory-constrained environments
df.persist(StorageLevel.MEMORY_AND_DISK)

# For serialized storage (less memory, more CPU)
df.persist(StorageLevel.MEMORY_ONLY_SER)

# For fault tolerance (replication)
df.persist(StorageLevel.MEMORY_AND_DISK_2)
```

**Trade-offs:**

| Storage Level | Memory | CPU | Disk I/O | Use Case |
|--------------|--------|-----|----------|----------|
| MEMORY_ONLY | High | Low | None | Fast, enough RAM |
| MEMORY_AND_DISK | Medium | Low | Medium | Limited RAM |
| MEMORY_ONLY_SER | Low | High | None | Very limited RAM |
| DISK_ONLY | None | Low | High | Extremely limited RAM |

### ✅ DO: Avoid collect() on Large Datasets

```python
# ❌ BAD - OOM error on large data
all_data = df.collect()  # Brings all data to driver

# ✅ GOOD - Sample or write to storage
sample = df.take(100)  # Get sample
df.show(20)  # Preview
df.write.parquet("output")  # Write to storage

# ✅ GOOD - Aggregations on cluster
count = df.count()  # Computed on executors
summary = df.describe().show()  # Aggregated on executors
```

### ✅ DO: Monitor Memory Usage

```python
# Check current memory usage
spark.sparkContext._jsc.sc().getExecutorMemoryStatus()

# Enable detailed memory logging
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.memory.fraction", "0.8")  # 80% for cache/execution
spark.conf.set("spark.memory.storageFraction", "0.5")  # 50% for cache
```

---

## 4. Data Quality

### ✅ DO: Validate Schema Before Processing

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define expected schema
expected_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("email", StringType(), nullable=False)
])

# Read with schema enforcement
df = spark.read.schema(expected_schema).csv("customers.csv")

# Validate schema
def validate_schema(df, expected_schema):
    if df.schema != expected_schema:
        raise ValueError(f"Schema mismatch! Expected: {expected_schema}, Got: {df.schema}")

validate_schema(df, expected_schema)
```

### ✅ DO: Handle Nulls Explicitly

```python
# ✅ Check for nulls
null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
])
null_counts.show()

# ✅ Handle nulls
df_clean = df.na.fill({
    "age": 0,
    "name": "Unknown",
    "salary": df.agg({"salary": "mean"}).first()[0]  # Fill with mean
})

# ✅ Drop rows with nulls in critical columns
df_clean = df.dropna(subset=["customer_id", "email"])
```

### ✅ DO: Add Data Quality Checks

```python
def quality_checks(df):
    """Run data quality validations"""
    checks = []

    # Check 1: No duplicate keys
    duplicate_count = df.groupBy("customer_id").count() \
        .filter(col("count") > 1).count()
    checks.append(("Duplicates", duplicate_count == 0))

    # Check 2: Valid email format
    invalid_emails = df.filter(~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")).count()
    checks.append(("Valid Emails", invalid_emails == 0))

    # Check 3: Age in valid range
    invalid_age = df.filter((col("age") < 0) | (col("age") > 120)).count()
    checks.append(("Valid Age", invalid_age == 0))

    # Print results
    for check_name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {check_name}")

    return all(passed for _, passed in checks)

# Run checks
if not quality_checks(df):
    raise ValueError("Data quality checks failed!")
```

---

## 5. Security

### ✅ DO: Avoid Hardcoding Credentials

```python
# ❌ BAD - Hardcoded credentials
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("user", "myuser") \
    .option("password", "mypassword") \  # Security risk!
    .load()

# ✅ GOOD - Use environment variables
import os
from dotenv import load_dotenv

load_dotenv()

df = spark.read \
    .format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("user", os.getenv("DB_USER")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()
```

### ✅ DO: Encrypt Sensitive Data

```python
from pyspark.sql.functions import sha2, md5

# Hash sensitive columns
df = df.withColumn("email_hash", sha2(col("email"), 256)) \
       .drop("email")  # Drop original sensitive column
```

---

## 6. Testing

### ✅ DO: Write Unit Tests for Transformations

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()

def test_filter_active_customers(spark):
    # Arrange
    data = [("1", "active"), ("2", "inactive"), ("3", "active")]
    df = spark.createDataFrame(data, ["id", "status"])

    # Act
    result = df.filter(col("status") == "active")

    # Assert
    assert result.count() == 2
    assert result.filter(col("id") == "1").count() == 1
```

### ✅ DO: Test with Sample Data

```python
def test_with_sample_data(spark):
    # Create sample data for testing
    sample_df = spark.createDataFrame([
        ("Alice", 25, 50000),
        ("Bob", 30, 60000),
        ("Charlie", 35, 70000)
    ], ["name", "age", "salary"])

    # Run transformation
    result = my_transformation_function(sample_df)

    # Validate result
    assert result.count() > 0
    assert "bonus" in result.columns
```

---

## 7. Deployment

### ✅ DO: Use spark-submit for Production

```bash
# ✅ GOOD - Production deployment
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=200 \
  --py-files dependencies.zip \
  main.py
```

### ✅ DO: Set Appropriate Resource Allocation

```python
# Cluster: 10 nodes, 16 cores each, 64GB RAM each

# Calculation:
# - Leave 1 core + 1GB for OS/Hadoop daemons per node
# - Available per node: 15 cores, 63GB RAM

spark-submit \
  --num-executors 30 \  # (10 nodes * 15 cores) / 5 cores per executor
  --executor-cores 5 \
  --executor-memory 21g \  # (63GB / 3 executors per node)
  --driver-memory 4g \
  my_job.py
```

### ✅ DO: Enable Adaptive Query Execution (Spark 3.0+)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

## 📊 Performance Checklist

### Before Running Production Jobs:

- [ ] Use DataFrame API instead of RDD
- [ ] Replace UDFs with built-in functions where possible
- [ ] Broadcast small tables in joins
- [ ] Set appropriate shuffle partitions
- [ ] Partition data for writes
- [ ] Use columnar formats (Parquet, ORC)
- [ ] Enable compression
- [ ] Cache frequently accessed DataFrames
- [ ] Unpersist cached data when done
- [ ] Monitor Spark UI for bottlenecks
- [ ] Set appropriate executor memory and cores
- [ ] Enable Adaptive Query Execution
- [ ] Use predicate pushdown
- [ ] Validate data quality
- [ ] Add logging and monitoring
- [ ] Test with sample data first
- [ ] Use configuration files
- [ ] Secure credentials properly

---

## 🎯 Summary

**Top 10 Best Practices:**

1. **Use DataFrame API** - Faster and optimized
2. **Avoid UDFs** - Use built-in functions
3. **Broadcast small tables** - Reduces shuffle
4. **Filter early** - Less data to process
5. **Cache wisely** - Reused DataFrames only
6. **Partition appropriately** - 128-256MB per partition
7. **Monitor Spark UI** - Identify bottlenecks
8. **Test thoroughly** - Unit tests with sample data
9. **Secure credentials** - Use environment variables
10. **Validate data quality** - Schema and null checks

---

**Next Steps**: See `trade_offs/` for detailed comparisons and `interview_prep/` for common questions!
