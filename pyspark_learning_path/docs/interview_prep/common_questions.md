# PySpark Interview Questions & Answers

## 📋 Table of Contents
1. [Fundamentals](#fundamentals)
2. [Architecture](#architecture)
3. [Performance & Optimization](#performance--optimization)
4. [DataFrames & SQL](#dataframes--sql)
5. [Memory Management](#memory-management)
6. [Troubleshooting](#troubleshooting)
7. [Coding Challenges](#coding-challenges)

---

## 1. Fundamentals

### Q1: What is the difference between transformation and action in Spark?

**Answer:**

**Transformations:**
- Return a new RDD/DataFrame
- Lazy evaluation (not executed immediately)
- Build up a DAG (Directed Acyclic Graph)
- Examples: `map()`, `filter()`, `select()`, `groupBy()`, `join()`

**Actions:**
- Return results to driver or write to storage
- Trigger execution of transformations
- Examples: `collect()`, `count()`, `show()`, `save()`, `take()`

```python
# Transformations (lazy - not executed)
df1 = df.filter(col("age") > 25)  # Transformation
df2 = df1.select("name", "age")   # Transformation

# Action (triggers execution of df1 and df2)
df2.show()  # Action
```

**Why this matters:**
- Spark can optimize the entire pipeline before execution
- Avoids unnecessary computation
- Enables fault tolerance through lineage

---

### Q2: Explain lazy evaluation in Spark

**Answer:**

**Lazy Evaluation** means transformations are not executed immediately. Instead, Spark builds an execution plan (DAG) and only executes when an action is called.

**Benefits:**
1. **Optimization**: Catalyst optimizer can optimize the entire pipeline
2. **Efficiency**: Avoids unnecessary intermediate results
3. **Pipeline fusion**: Combines multiple operations
4. **Predicate pushdown**: Filters at data source

**Example:**
```python
# Step 1: Define transformations (not executed)
df = spark.read.csv("large_file.csv")  # Not read yet!
filtered = df.filter(col("status") == "active")  # Not executed
selected = filtered.select("id", "name")  # Not executed

# Step 2: Action triggers execution of entire pipeline
result = selected.count()  # NOW everything executes
```

**Interview Tip:** Explain that this allows Spark to:
- Skip reading unnecessary columns (column pruning)
- Push filters to data source (predicate pushdown)
- Combine multiple operations into single stage

---

### Q3: When would you use RDD instead of DataFrame?

**Answer:**

Use **RDD** when:
1. You need fine-grained control over physical data layout
2. Working with unstructured data (text processing)
3. Complex custom transformations not available in DataFrame API
4. Need to manipulate data at the row level with full control

Use **DataFrame** for (99% of cases):
1. Structured/semi-structured data
2. SQL-like operations
3. Need optimization (Catalyst + Tungsten)
4. Better performance
5. Easier to read and maintain

**Example where RDD makes sense:**
```python
# Complex text processing
text_rdd = sc.textFile("logs.txt")
parsed = text_rdd.map(lambda line: custom_complex_parser(line)) \
                 .filter(lambda x: complex_condition(x))
```

**Example where DataFrame is better:**
```python
# Structured data processing
df = spark.read.parquet("users.parquet")
result = df.filter(col("age") > 25) \
          .groupBy("city") \
          .agg(avg("salary"))
```

---

## 2. Architecture

### Q4: Explain Spark's architecture (Driver, Executor, Cluster Manager)

**Answer:**

```
┌──────────────────────┐
│   Driver Program     │
│   - SparkContext     │
│   - Task scheduling  │
│   - Result gathering │
└──────┬───────────────┘
       │
       ▼
┌──────────────────────┐
│  Cluster Manager     │
│  (YARN/Mesos/K8s)    │
│  - Resource alloc    │
└──────┬───────────────┘
       │
   ┌───┴────┬────┬────┐
   ▼        ▼    ▼    ▼
┌─────┐ ┌─────┐ ┌─────┐
│Exec1│ │Exec2│ │Exec3│
│Task │ │Task │ │Task │
│Task │ │Task │ │Task │
│Cache│ │Cache│ │Cache│
└─────┘ └─────┘ └─────┘
```

**Components:**

1. **Driver:**
   - Runs main() function
   - Creates SparkContext
   - Converts RDD/DataFrame operations into DAG
   - Schedules tasks on executors
   - Collects results

2. **Cluster Manager:**
   - Allocates resources (CPU, memory)
   - Types: YARN, Mesos, Kubernetes, Standalone

3. **Executors:**
   - Worker processes running on cluster nodes
   - Run tasks sent by driver
   - Cache data in memory
   - Return results to driver

**Interview Follow-up:** Explain the execution flow:
1. Driver creates RDD/DataFrame transformations
2. Transformations build a DAG
3. Action triggers DAG submission to DAG Scheduler
4. DAG Scheduler splits DAG into stages
5. Task Scheduler sends tasks to executors
6. Executors run tasks and return results

---

### Q5: What is the difference between client and cluster deploy mode?

**Answer:**

**Client Mode:**
- Driver runs on the machine that submits the job
- Good for interactive work (notebooks, testing)
- Driver is outside the cluster

```bash
spark-submit --deploy-mode client my_job.py
```

**Pros:** Easy to debug, see logs immediately
**Cons:** Driver failure kills job, driver not fault-tolerant

**Cluster Mode:**
- Driver runs on one of the cluster nodes
- Good for production jobs
- Driver is inside the cluster

```bash
spark-submit --deploy-mode cluster my_job.py
```

**Pros:** Driver is fault-tolerant, better for production
**Cons:** Harder to debug, logs on cluster node

**When to use:**
- **Client**: Development, testing, Jupyter notebooks
- **Cluster**: Production scheduled jobs, long-running jobs

---

## 3. Performance & Optimization

### Q6: How do you optimize a slow Spark job?

**Answer (Step-by-step approach):**

**Step 1: Identify the bottleneck using Spark UI**
- Check which stage takes the longest
- Look for data skew (some tasks much slower)
- Check shuffle read/write sizes
- Monitor memory usage

**Step 2: Common optimizations**

1. **Use DataFrame API instead of RDD**
```python
# ❌ Slow
rdd.map(...).filter(...).collect()
# ✅ Fast
df.select(...).filter(...).show()
```

2. **Avoid UDFs, use built-in functions**
```python
# ❌ Slow
@udf
def upper(s): return s.upper()
# ✅ Fast (5x faster)
from pyspark.sql.functions import upper
```

3. **Broadcast small tables in joins**
```python
# ✅ Fast for small dimension tables
large_df.join(broadcast(small_df), "key")
```

4. **Optimize shuffle partitions**
```python
# Default is 200, tune based on data size
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

5. **Cache frequently accessed DataFrames**
```python
df_cached = df.filter(...).cache()
# Use multiple times
df_cached.count()
df_cached.show()
```

6. **Filter early in the pipeline**
```python
# ✅ Filter before expensive operations
df.filter(date >= "2024-01-01").join(...)
```

7. **Use appropriate file formats**
```python
# ✅ Use Parquet/ORC with compression
df.write.parquet("output", compression="snappy")
```

**Step 3: Monitor and iterate**
- Re-run with changes
- Compare execution times in Spark UI
- Check if shuffle size reduced

---

### Q7: Explain the different types of joins in Spark and their trade-offs

**Answer:**

**1. Broadcast Hash Join**
- **When:** One table < 10MB (configurable)
- **How:** Small table broadcast to all executors
- **Performance:** Fastest, no shuffle
- **Trade-off:** Limited by broadcast size

```python
# Small table < 10MB
large_df.join(broadcast(small_df), "key")
```

**2. Sort-Merge Join**
- **When:** Both tables are large
- **How:** Sort both sides, then merge
- **Performance:** Good for large tables
- **Trade-off:** Requires sorting (expensive)

```python
# Happens automatically for large-large joins
large_df1.join(large_df2, "key")
```

**3. Shuffle Hash Join**
- **When:** Medium-sized tables
- **How:** Shuffle and build hash table
- **Performance:** Middle ground
- **Trade-off:** Builds hash table in memory

**Comparison Table:**

| Join Type | Best For | Shuffle | Memory | Speed |
|-----------|----------|---------|--------|-------|
| Broadcast | Small < 10MB | ❌ No | High | ⚡ Fastest |
| Sort-Merge | Large tables | ✅ Yes | Medium | 🐢 Slower |
| Shuffle Hash | Medium tables | ✅ Yes | High | ⚡ Fast |

**How to choose:**
1. If one table < 10MB → Broadcast
2. If both large and have sortable keys → Sort-Merge
3. Otherwise → Shuffle Hash

---

### Q8: What is data skew and how do you handle it?

**Answer:**

**Data Skew** occurs when data is unevenly distributed across partitions, causing some tasks to take much longer than others.

**Symptoms:**
- One or few tasks take 10x-100x longer
- Spark UI shows uneven task durations
- Some executors have high shuffle read
- Stragglers in stages

**Example:**
```python
# Skewed data: 90% of users in "New York"
user_df.groupBy("city").count()
# One partition processes 90% of data!
```

**Solutions:**

**1. Salting (Add random key)**
```python
from pyspark.sql.functions import rand, concat, lit

# Add salt to skewed key
salted_df = df.withColumn("salted_key",
    concat(col("city"), lit("_"), (rand() * 10).cast("int")))

# Join on salted key
result = salted_df.join(other_df, "salted_key")
```

**2. Broadcast the skewed table (if small enough)**
```python
# If skewed table is small
result = large_df.join(broadcast(skewed_df), "key")
```

**3. Use Adaptive Query Execution (Spark 3.0+)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**4. Repartition by multiple columns**
```python
# Instead of just city
df.repartition("city")  # Skewed

# Use multiple columns
df.repartition("city", "date")  # Better distribution
```

**Interview Tip:** Mention that you would:
1. Check Spark UI to identify skew
2. Analyze data distribution
3. Choose appropriate solution based on data characteristics

---

### Q9: What is the difference between cache() and persist()?

**Answer:**

**cache():**
- Shorthand for `persist(StorageLevel.MEMORY_ONLY)`
- Stores data in memory only
- Evicts data if memory is full

**persist():**
- Allows custom storage levels
- More control over caching strategy

**Storage Levels:**

```python
from pyspark import StorageLevel

# Memory only (same as cache())
df.persist(StorageLevel.MEMORY_ONLY)

# Memory + Disk (if memory full, spill to disk)
df.persist(StorageLevel.MEMORY_AND_DISK)

# Serialized (compressed in memory)
df.persist(StorageLevel.MEMORY_ONLY_SER)

# Disk only (no memory)
df.persist(StorageLevel.DISK_ONLY)

# With replication (fault tolerance)
df.persist(StorageLevel.MEMORY_AND_DISK_2)
```

**When to use each:**

| Scenario | Storage Level | Reason |
|----------|--------------|--------|
| Enough memory | MEMORY_ONLY | Fastest |
| Limited memory | MEMORY_AND_DISK | Prevents recomputation |
| Very limited memory | MEMORY_ONLY_SER | Saves memory (uses CPU) |
| Critical data | MEMORY_AND_DISK_2 | Replication for fault tolerance |

**Important:**
```python
# ✅ Always unpersist when done
df.unpersist()
```

---

## 4. DataFrames & SQL

### Q10: What are window functions in Spark SQL? Give examples.

**Answer:**

**Window Functions** perform calculations across a set of rows related to the current row.

**Types:**

**1. Ranking Functions**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("row_number", row_number().over(window_spec)) \
  .withColumn("rank", rank().over(window_spec)) \
  .withColumn("dense_rank", dense_rank().over(window_spec))
```

**Output example:**
```
department | salary | row_number | rank | dense_rank
-----------|--------|------------|------|------------
Sales      | 10000  | 1          | 1    | 1
Sales      | 10000  | 2          | 1    | 1
Sales      | 9000   | 3          | 3    | 2
```

**2. Analytic Functions**
```python
from pyspark.sql.functions import lag, lead

window_spec = Window.partitionBy("user_id").orderBy("date")

df.withColumn("prev_amount", lag("amount", 1).over(window_spec)) \
  .withColumn("next_amount", lead("amount", 1).over(window_spec))
```

**3. Aggregate Functions**
```python
from pyspark.sql.functions import sum, avg

window_spec = Window.partitionBy("department") \
                   .orderBy("date") \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("cumulative_sales", sum("sales").over(window_spec)) \
  .withColumn("running_avg", avg("sales").over(window_spec))
```

**Use Cases:**
- Top N per group
- Running totals
- Moving averages
- Comparing current vs previous values

---

## 5. Memory Management

### Q11: How do you troubleshoot OOM (Out of Memory) errors in Spark?

**Answer:**

**Common causes and solutions:**

**1. collect() on large DataFrame**
```python
# ❌ Causes OOM
df.collect()  # Brings all data to driver

# ✅ Solutions
df.take(100)  # Get sample
df.write.parquet("output")  # Write to storage
```

**2. Insufficient executor memory**
```bash
# ✅ Increase executor memory
spark-submit --executor-memory 8g my_job.py
```

**3. Too many cached DataFrames**
```python
# ✅ Unpersist unused DataFrames
df1.unpersist()
df2.unpersist()
```

**4. Skewed data in single partition**
```python
# ✅ Repartition to distribute evenly
df = df.repartition(200)
```

**5. Serialized objects too large**
```python
# ✅ Use broadcast for large variables
broadcast_var = sc.broadcast(large_lookup_dict)
```

**Configuration tuning:**
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.memory.fraction", "0.8")  # 80% for cache/execution
spark.conf.set("spark.memory.storageFraction", "0.5")  # 50% for storage
```

**Debugging steps:**
1. Check Spark UI → Executors → Memory usage
2. Check Spark UI → Stages → Task metrics
3. Enable verbose GC logging
4. Reduce partition size or increase partitions
5. Use serialized storage levels

---

## 6. Troubleshooting

### Q12: How do you debug a failing Spark job?

**Answer:**

**Step-by-step debugging approach:**

**1. Check Spark UI (http://localhost:4040)**
- Jobs tab: Which job failed?
- Stages tab: Which stage failed?
- Tasks tab: Which tasks failed?
- Executors tab: Resource usage

**2. Check logs**
```bash
# Driver logs
cat driver.log | grep ERROR

# Executor logs
yarn logs -applicationId app_123 | grep ERROR
```

**3. Common issues and solutions**

**Issue: Task fails with NullPointerException**
```python
# ✅ Add null checks
df.filter(col("value").isNotNull())
```

**Issue: Shuffle failures**
```python
# ✅ Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")

# ✅ Increase shuffle memory
spark.conf.set("spark.shuffle.memoryFraction", "0.4")
```

**Issue: Container killed by YARN**
```python
# ✅ Increase memory overhead
spark.conf.set("spark.yarn.executor.memoryOverhead", "2g")
```

**4. Enable detailed logging**
```python
spark.sparkContext.setLogLevel("DEBUG")
```

**5. Test with sample data**
```python
# Debug with small sample
sample_df = df.sample(0.01)  # 1% sample
result = my_transformation(sample_df)
```

---

## 7. Coding Challenges

### Q13: Write PySpark code to find the top 3 highest paid employees in each department

**Answer:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Sample data
data = [
    ("Sales", "John", 10000),
    ("Sales", "Jane", 12000),
    ("Sales", "Bob", 9000),
    ("Sales", "Alice", 11000),
    ("IT", "Charlie", 15000),
    ("IT", "David", 14000),
    ("IT", "Eve", 16000),
]

df = spark.createDataFrame(data, ["department", "name", "salary"])

# Solution using window function
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

result = df.withColumn("rank", row_number().over(window_spec)) \
           .filter(col("rank") <= 3) \
           .drop("rank")

result.show()
```

**Output:**
```
department | name    | salary
-----------|---------|--------
IT         | Eve     | 16000
IT         | Charlie | 15000
IT         | David   | 14000
Sales      | Jane    | 12000
Sales      | Alice   | 11000
Sales      | John    | 10000
```

---

### Q14: How would you deduplicate data based on multiple columns?

**Answer:**

```python
# Method 1: Using dropDuplicates()
deduplicated = df.dropDuplicates(["customer_id", "order_date"])

# Method 2: Keep latest record using window function
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

window_spec = Window.partitionBy("customer_id", "order_date") \
                    .orderBy(desc("timestamp"))

result = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")

# Method 3: Using SQL
df.createOrReplaceTempView("orders")
result = spark.sql("""
    SELECT DISTINCT customer_id, order_date, amount
    FROM orders
""")
```

---

## 🎯 Key Interview Tips

1. **Always explain the "why"** - Don't just answer how, explain why Spark works that way
2. **Mention trade-offs** - Every solution has pros and cons
3. **Use Spark UI** - Mention monitoring and debugging
4. **Consider scale** - Think about how solution scales to TB/PB data
5. **Optimize first** - Prefer DataFrame API, built-in functions, broadcast joins
6. **Real examples** - Share experiences from actual projects

---

## 📚 Additional Study Topics

- Catalyst Optimizer internals
- Tungsten execution engine
- Adaptive Query Execution (AQE)
- Structured Streaming
- MLlib for machine learning
- Delta Lake / Data Lake concepts

---

**Practice Makes Perfect!** Work through the exercises in `exercises/` directory for hands-on preparation.
