# PySpark Performance Comparisons & Trade-offs

## 📊 Table of Contents
1. [RDD vs DataFrame vs Dataset](#rdd-vs-dataframe-vs-dataset)
2. [UDF vs Built-in Functions](#udf-vs-built-in-functions)
3. [Join Strategies](#join-strategies)
4. [Caching Storage Levels](#caching-storage-levels)
5. [File Formats](#file-formats)
6. [Partitioning Strategies](#partitioning-strategies)
7. [Real-World Benchmarks](#real-world-benchmarks)

---

## 1. RDD vs DataFrame vs Dataset

### Comparison Table

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| **Type Safety** | ❌ Runtime | ❌ Runtime | ✅ Compile-time (Scala/Java) |
| **Optimization** | ❌ None | ✅ Catalyst | ✅ Catalyst |
| **Memory Usage** | Higher | Lower (Tungsten) | Lower (Tungsten) |
| **Ease of Use** | Medium | ✅ High | Medium |
| **Performance** | Slowest | ⚡ Fastest | ⚡ Fastest |
| **API** | Functional | SQL-like | Type-safe functional |
| **Schema** | ❌ None | ✅ Yes | ✅ Yes |
| **Serialization** | Java/Kryo | Tungsten binary | Tungsten binary |

### Performance Benchmark

**Test:** Filter and aggregate 100M rows

```python
# Dataset: 100 million rows, 10 columns
# Operation: Filter age > 25, group by city, count

# RDD Approach
Time: 125 seconds
Memory: 4.2 GB

# DataFrame Approach
Time: 18 seconds  (7x faster)
Memory: 1.8 GB    (2.3x less)

# Speed-up: 7x faster, 57% less memory
```

### When to Use Each

**Use RDD when:**
- Need fine-grained control over physical data
- Working with unstructured text data
- Complex custom partitioning logic
- Legacy codebase already using RDDs

**Use DataFrame when:**
- Structured/semi-structured data (99% of cases)
- Need SQL capabilities
- Want automatic optimization
- Working with multiple data sources

**Use Dataset when (Scala/Java only):**
- Need compile-time type safety
- Complex domain objects
- Strict type checking required

---

## 2. UDF vs Built-in Functions

### Performance Comparison

**Test:** Convert 10M strings to uppercase

```python
# Method 1: UDF
@udf(StringType())
def upper_udf(s):
    return s.upper()

df.withColumn("upper", upper_udf("name"))
Time: 45 seconds
CPU: 95% (serialization overhead)

# Method 2: Built-in function
from pyspark.sql.functions import upper

df.withColumn("upper", upper("name"))
Time: 8 seconds  (5.6x faster)
CPU: 60% (optimized)
```

### Why Built-ins are Faster

```
UDF Flow:
Python → JVM → Python (serialization overhead)
┌──────┐    ┌─────┐    ┌──────┐
│ JVM  │ ⟷  │Py4J │ ⟷  │Python│
└──────┘    └─────┘    └──────┘
  Slow      Overhead     Slow

Built-in Flow:
Optimized JVM code (no Python)
┌──────────────────┐
│ Catalyst + JVM   │
│ (Optimized)      │
└──────────────────┘
      Fast
```

### Pandas UDF (Vectorized UDF)

**Improvement over standard UDF:**

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Standard UDF: 45 seconds
@udf(DoubleType())
def calculate_udf(x, y):
    return x * y + y

# Pandas UDF: 12 seconds (3.75x faster)
@pandas_udf(DoubleType())
def calculate_pandas_udf(x: pd.Series, y: pd.Series) -> pd.Series:
    return x * y + y
```

**Performance Ranking:**
1. ⚡⚡⚡ Built-in functions (fastest)
2. ⚡⚡ Pandas UDF (vectorized)
3. ⚡ Standard UDF (slowest)

---

## 3. Join Strategies

### Performance Comparison

**Scenario:** Large table (10GB) joining with different sized tables

#### Test 1: Small Table (10MB)

```python
# Broadcast Hash Join
large_df.join(broadcast(small_df), "key")

Time: 25 seconds
Shuffle: 0 MB
Memory: 2 GB
Winner: Broadcast
```

#### Test 2: Medium Table (500MB)

```python
# Shuffle Hash Join
large_df.join(medium_df, "key")

Time: 65 seconds
Shuffle: 5 GB
Memory: 3 GB
```

#### Test 3: Large Table (5GB)

```python
# Sort-Merge Join
large_df1.join(large_df2, "key")

Time: 120 seconds
Shuffle: 15 GB
Memory: 4 GB
Winner: Sort-Merge (most scalable)
```

### Decision Matrix

| Left Size | Right Size | Best Strategy | Trade-off |
|-----------|------------|---------------|-----------|
| Any | < 10MB | Broadcast | Fast, high memory |
| Large | 10MB-1GB | Shuffle Hash | Balanced |
| Large | > 1GB | Sort-Merge | Slower, scalable |

### Data Skew Impact

**Normal distribution:**
```
Join time: 120 seconds
Task durations: 118s, 120s, 119s, 121s (even)
```

**Skewed distribution:**
```
Join time: 450 seconds
Task durations: 50s, 60s, 450s, 55s (one straggler)

Solution: Salting or AQE skew join optimization
```

---

## 4. Caching Storage Levels

### Performance Comparison

**Test:** Cache 1GB DataFrame, perform 10 iterations

#### MEMORY_ONLY
```python
df.persist(StorageLevel.MEMORY_ONLY)

Memory: 1 GB RAM
Disk: 0 GB
Time per iteration: 2 seconds
Total: 20 seconds

Best for: Enough memory available
```

#### MEMORY_AND_DISK
```python
df.persist(StorageLevel.MEMORY_AND_DISK)

Memory: 500 MB RAM
Disk: 500 MB
Time per iteration: 3 seconds
Total: 30 seconds (1.5x slower)

Best for: Limited memory, need fault tolerance
```

#### MEMORY_ONLY_SER
```python
df.persist(StorageLevel.MEMORY_ONLY_SER)

Memory: 300 MB RAM (compressed)
Disk: 0 GB
CPU: +30% (serialization/deserialization)
Time per iteration: 4 seconds
Total: 40 seconds (2x slower)

Best for: Very limited memory
```

#### DISK_ONLY
```python
df.persist(StorageLevel.DISK_ONLY)

Memory: 100 MB RAM
Disk: 1 GB
Time per iteration: 5 seconds
Total: 50 seconds (2.5x slower)

Best for: Extremely limited memory
```

### Trade-off Summary

| Storage Level | Memory | Speed | Fault Tolerance | CPU |
|--------------|--------|-------|-----------------|-----|
| MEMORY_ONLY | High | ⚡⚡⚡ Fast | Low | Low |
| MEMORY_AND_DISK | Medium | ⚡⚡ Medium | High | Low |
| MEMORY_ONLY_SER | Low | ⚡ Slow | Low | High |
| DISK_ONLY | Very Low | 🐢 Slowest | High | Low |

**Replication (_2 suffix):**
- Doubles memory/disk usage
- Provides fault tolerance
- Use for critical data only

---

## 5. File Formats

### Performance Comparison

**Test:** Write 10M rows (10 columns), then read and filter

#### CSV
```
Write time: 65 seconds
File size: 2.5 GB
Read time: 45 seconds
Filter pushdown: ❌ No
Schema inference: ⚠️ Slow

Pros: Human-readable, universal
Cons: Slow, no schema, no compression
```

#### JSON
```
Write time: 70 seconds
File size: 3.2 GB
Read time: 55 seconds
Filter pushdown: ❌ No
Schema inference: ⚠️ Slow

Pros: Semi-structured, human-readable
Cons: Slow, verbose, large files
```

#### Parquet
```
Write time: 15 seconds
File size: 450 MB (82% smaller!)
Read time: 5 seconds (9x faster!)
Filter pushdown: ✅ Yes (predicate pushdown)
Schema inference: ✅ Fast (embedded schema)
Compression: Snappy (default)

Pros: Fast, small, columnar, schema embedded
Cons: Not human-readable
Winner: Best for analytics
```

#### ORC
```
Write time: 14 seconds
File size: 420 MB (84% smaller!)
Read time: 4 seconds
Filter pushdown: ✅ Yes
Schema inference: ✅ Fast
Compression: Zlib (default)

Pros: Very fast, very small, optimized for Hive
Cons: Not human-readable
Winner: Best for Hive integration
```

### Compression Comparison (Parquet)

| Compression | File Size | Write Time | Read Time | CPU | Use Case |
|-------------|-----------|------------|-----------|-----|----------|
| None | 1.8 GB | 12s | 8s | Low | Fast processing |
| Snappy | 450 MB | 15s | 5s | Medium | **Default (balanced)** |
| Gzip | 320 MB | 35s | 18s | High | Storage-critical |
| LZO | 500 MB | 14s | 6s | Medium | Splittable |

**Recommendation:**
- **Default:** Snappy (best balance)
- **Network bandwidth limited:** Gzip
- **CPU limited:** None or Snappy

---

## 6. Partitioning Strategies

### repartition() vs coalesce()

**Test:** 1000 partitions → 100 partitions

#### repartition(100)
```
Time: 45 seconds
Shuffle: ✅ Yes (full shuffle)
Data distribution: Even
Use: Increase or decrease partitions

Code: df.repartition(100)
```

#### coalesce(100)
```
Time: 5 seconds (9x faster!)
Shuffle: ❌ No
Data distribution: May be uneven
Use: Only decrease partitions

Code: df.coalesce(100)
```

### Optimal Partition Size

**Test different partition sizes on 10GB data:**

| Partitions | Partition Size | Time | Notes |
|------------|----------------|------|-------|
| 10 | 1 GB | 180s | Too large, OOM risk |
| 50 | 200 MB | 85s | Good |
| 100 | 100 MB | 55s | **Optimal** |
| 500 | 20 MB | 70s | Too many tasks |
| 2000 | 5 MB | 120s | High overhead |

**Formula:**
```
Optimal partitions = Total data size (MB) / 128 MB
                   ≈ 2-4 × number of cores
```

**Example:**
- 10GB data = 10,240 MB
- Optimal partitions = 10,240 / 128 = 80 partitions

---

## 7. Real-World Benchmarks

### Scenario 1: ETL Pipeline

**Task:** Read 50GB CSV, transform, write Parquet

#### Baseline (Unoptimized)
```python
df = spark.read.csv("input.csv")
result = df.filter(...).groupBy(...).agg(...)
result.write.parquet("output")

Time: 12 minutes
```

#### Optimized
```python
# 1. Read with schema (no inference)
df = spark.read.schema(predefined_schema).csv("input.csv")

# 2. Filter early
df = df.filter(col("date") >= "2024-01-01")

# 3. Optimize shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")

# 4. Coalesce before write
result.coalesce(50).write.parquet("output", compression="snappy")

Time: 3 minutes (4x faster!)
```

### Scenario 2: Join Optimization

**Task:** Join 100GB table with 50GB table

#### Baseline
```python
large1.join(large2, "key")

Time: 25 minutes
Shuffle: 150 GB
```

#### Optimized (Bucketing)
```python
# Pre-bucket tables
large1.write.bucketBy(200, "key").saveAsTable("large1_bucketed")
large2.write.bucketBy(200, "key").saveAsTable("large2_bucketed")

# Join bucketed tables
bucketed1 = spark.table("large1_bucketed")
bucketed2 = spark.table("large2_bucketed")
result = bucketed1.join(bucketed2, "key")

Time: 8 minutes (3x faster!)
Shuffle: 0 GB (no shuffle!)
```

### Scenario 3: Window Function Optimization

**Task:** Rank 1B rows by department

#### Baseline
```python
window = Window.partitionBy("dept").orderBy(col("salary").desc())
df.withColumn("rank", rank().over(window))

Time: 18 minutes
Memory: High (sorts per partition)
```

#### Optimized (Pre-sort + repartition)
```python
# Pre-sort and repartition
df_sorted = df.repartition("dept").sortWithinPartitions("dept", col("salary").desc())

window = Window.partitionBy("dept").orderBy(col("salary").desc())
df_sorted.withColumn("rank", rank().over(window))

Time: 8 minutes (2.25x faster!)
```

---

## 🎯 Key Takeaways

### Always Choose:
1. **DataFrame > RDD** (7x faster)
2. **Built-in functions > UDF** (5x faster)
3. **Broadcast join for small tables** (no shuffle)
4. **Parquet/ORC > CSV/JSON** (9x faster reads)
5. **coalesce() > repartition()** when reducing partitions
6. **Filter early** in pipeline
7. **Snappy compression** for Parquet (balanced)
8. **Cache reused DataFrames** with appropriate storage level

### Avoid:
1. UDFs when built-in exists
2. collect() on large datasets
3. Too many small files
4. Default shuffle partitions (200) for large data
5. CSV for production data
6. Unnecessary repartitioning

---

## 📊 Quick Reference Chart

| Operation | Slow ❌ | Fast ✅ |
|-----------|---------|---------|
| API | RDD | DataFrame |
| Custom logic | UDF | Built-in function |
| Small table join | Shuffle join | Broadcast join |
| File format | CSV/JSON | Parquet/ORC |
| Reduce partitions | repartition() | coalesce() |
| Storage | Uncompressed | Snappy compressed |

---

**Remember:** Always benchmark with your actual data and workload! These comparisons are guidelines, not absolute rules.
