# PySpark Complete Learning Path
### From Zero to Hero: Theory + Practical + Best Practices

A comprehensive, hands-on learning resource for mastering Apache Spark with Python. This repository provides a structured learning path from fundamentals to advanced concepts, including theory, practical examples, performance optimization, trade-offs, and interview preparation.

## 📚 Table of Contents

- [Learning Path Overview](#learning-path-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Learning Modules](#learning-modules)
- [Best Practices](#best-practices)
- [Performance Optimization](#performance-optimization)
- [Interview Preparation](#interview-preparation)

---

## 🎯 Learning Path Overview

```
Level 1: Beginner (Weeks 1-2)
├── What is Spark & PySpark?
├── RDD Fundamentals
├── Basic Transformations & Actions
└── Hands-on: Word Count, Data Filtering

Level 2: Intermediate (Weeks 3-4)
├── DataFrames & Datasets
├── Spark SQL
├── Data Sources (CSV, JSON, Parquet)
├── Joins & Aggregations
└── Hands-on: Data Analysis Projects

Level 3: Advanced (Weeks 5-6)
├── Advanced Transformations
├── Window Functions
├── UDFs & Pandas UDFs
├── Partitioning Strategies
└── Hands-on: Complex ETL Pipeline

Level 4: Optimization (Weeks 7-8)
├── Performance Tuning
├── Memory Management
├── Catalyst Optimizer
├── Adaptive Query Execution
├── Caching & Persistence
└── Hands-on: Optimization Challenges

Level 5: Production (Weeks 9-10)
├── Spark Streaming
├── Structured Streaming
├── Machine Learning with MLlib
├── Deployment Strategies
├── Monitoring & Debugging
└── Hands-on: Real-time Data Pipeline
```

---

## ✅ Prerequisites

### Required Knowledge
- Python basics (variables, functions, loops, classes)
- SQL fundamentals
- Basic understanding of distributed systems (helpful but not mandatory)

### Software Requirements
- Python 3.8+
- Java 8 or 11
- Apache Spark 3.5.0
- Jupyter Notebook

---

## 🚀 Setup Instructions

### Option 1: Local Setup

```bash
# Clone the repository
git clone https://github.com/Gowda-kiran/ecommerce_data_pipeline.git
cd pyspark_learning_path

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Notebook
jupyter notebook
```

### Option 2: Docker Setup

```bash
# Build and run
docker-compose up -d

# Access Jupyter at http://localhost:8888
```

---

## 📖 Learning Modules

### Module 1: Beginner Level

#### 1.1 Introduction to Spark
- **Theory**: Distributed computing, Spark architecture, Components
- **Notebook**: `notebooks/01_beginner/01_intro_to_spark.ipynb`
- **Topics**:
  - What is Apache Spark?
  - Spark vs Hadoop MapReduce
  - Spark Architecture (Driver, Executors, Cluster Manager)
  - Spark Components (Core, SQL, Streaming, MLlib, GraphX)

#### 1.2 RDD Fundamentals
- **Theory**: Resilient Distributed Datasets, Lazy evaluation
- **Notebook**: `notebooks/01_beginner/02_rdd_basics.ipynb`
- **Topics**:
  - Creating RDDs
  - Transformations (map, filter, flatMap)
  - Actions (collect, count, reduce)
  - Lazy evaluation and lineage

#### 1.3 Basic Operations
- **Practical**: `notebooks/01_beginner/03_basic_operations.ipynb`
- **Examples**:
  - Word count
  - Data filtering
  - Key-value pairs
  - Simple aggregations

### Module 2: Intermediate Level

#### 2.1 DataFrames & Spark SQL
- **Theory**: DataFrame API, Catalyst optimizer
- **Notebook**: `notebooks/02_intermediate/01_dataframes_intro.ipynb`
- **Topics**:
  - RDD vs DataFrame vs Dataset
  - Creating DataFrames
  - Schema definition
  - DataFrame operations

#### 2.2 Data Sources
- **Practical**: `notebooks/02_intermediate/02_data_sources.ipynb`
- **Topics**:
  - Reading CSV, JSON, Parquet
  - Write modes (overwrite, append, ignore)
  - Partitioning
  - Compression

#### 2.3 Transformations
- **Notebook**: `notebooks/02_intermediate/03_transformations.ipynb`
- **Topics**:
  - select, filter, where
  - groupBy, agg
  - orderBy, sort
  - withColumn, drop, rename

#### 2.4 Joins & Aggregations
- **Practical**: `notebooks/02_intermediate/04_joins_aggregations.ipynb`
- **Topics**:
  - Inner, outer, left, right joins
  - Broadcast joins
  - Aggregation functions
  - Group by operations

### Module 3: Advanced Level

#### 3.1 Window Functions
- **Notebook**: `notebooks/03_advanced/01_window_functions.ipynb`
- **Topics**:
  - Row number, rank, dense_rank
  - Lead, lag
  - Cumulative aggregations
  - Partitioning and ordering

#### 3.2 User Defined Functions (UDFs)
- **Notebook**: `notebooks/03_advanced/02_udfs.ipynb`
- **Topics**:
  - Standard UDFs
  - Pandas UDFs (Vectorized)
  - Performance comparison
  - When to use UDFs

#### 3.3 Advanced Joins
- **Notebook**: `notebooks/03_advanced/03_advanced_joins.ipynb`
- **Topics**:
  - Broadcast hash join
  - Sort-merge join
  - Skew joins
  - Join optimization

#### 3.4 Partitioning Strategies
- **Notebook**: `notebooks/03_advanced/04_partitioning.ipynb`
- **Topics**:
  - repartition vs coalesce
  - Custom partitioning
  - Partition pruning
  - Optimal partition size

### Module 4: Performance Optimization

#### 4.1 Caching & Persistence
- **Notebook**: `notebooks/04_optimization/01_caching.ipynb`
- **Topics**:
  - cache() vs persist()
  - Storage levels
  - When to cache
  - Unpersist best practices

#### 4.2 Performance Tuning
- **Notebook**: `notebooks/04_optimization/02_performance_tuning.ipynb`
- **Topics**:
  - Executor memory and cores
  - Shuffle partitions
  - Broadcast threshold
  - Adaptive Query Execution (AQE)

#### 4.3 Memory Management
- **Notebook**: `notebooks/04_optimization/03_memory_management.ipynb`
- **Topics**:
  - Memory allocation
  - Spill to disk
  - OOM errors and solutions
  - Garbage collection tuning

#### 4.4 Query Optimization
- **Notebook**: `notebooks/04_optimization/04_query_optimization.ipynb`
- **Topics**:
  - Catalyst optimizer
  - Predicate pushdown
  - Column pruning
  - Explain plans

---

## 🎓 Best Practices

### 1. Code Organization
✅ Use meaningful variable names
✅ Break complex transformations into steps
✅ Add comments for business logic
✅ Use configuration files for parameters

### 2. Performance
✅ Prefer DataFrame API over RDD API
✅ Use built-in functions instead of UDFs
✅ Partition data appropriately
✅ Cache intermediate results when reused
✅ Use broadcast joins for small tables
✅ Avoid collect() on large datasets

### 3. Data Quality
✅ Validate schema before processing
✅ Handle null values explicitly
✅ Use data quality checks
✅ Log data statistics

### 4. Resource Management
✅ Set appropriate executor memory and cores
✅ Monitor Spark UI for bottlenecks
✅ Clean up cached DataFrames
✅ Use dynamic allocation when possible

---

## ⚖️ Trade-offs & Comparisons

### RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| **Type Safety** | ❌ No | ❌ No | ✅ Yes (Scala/Java) |
| **Performance** | ⚠️ Medium | ✅ High | ✅ High |
| **Optimization** | ❌ No | ✅ Catalyst | ✅ Catalyst |
| **Ease of Use** | ⚠️ Medium | ✅ High | ⚠️ Medium |
| **Best For** | Low-level control | SQL-like operations | Type-safe transformations |

### Caching Storage Levels

| Level | Memory | Disk | Serialized | Replication |
|-------|--------|------|------------|-------------|
| **MEMORY_ONLY** | ✅ | ❌ | ❌ | ❌ |
| **MEMORY_AND_DISK** | ✅ | ✅ | ❌ | ❌ |
| **MEMORY_ONLY_SER** | ✅ | ❌ | ✅ | ❌ |
| **DISK_ONLY** | ❌ | ✅ | ❌ | ❌ |
| **MEMORY_AND_DISK_2** | ✅ | ✅ | ❌ | ✅ |

**Trade-offs**:
- Serialized = Lower memory, higher CPU
- Replication = Better fault tolerance, double storage

### Join Strategies

| Join Type | Best For | Trade-off |
|-----------|----------|-----------|
| **Broadcast** | Small table (<10MB) | Fast, high memory |
| **Sort-merge** | Large tables | Slower, scalable |
| **Shuffle hash** | Medium tables | Balanced |

---

## 🚀 Performance Optimization Checklist

### 1. Data Ingestion
- [ ] Use columnar formats (Parquet, ORC)
- [ ] Enable compression (Snappy, LZO)
- [ ] Partition large datasets
- [ ] Predicate pushdown when reading

### 2. Transformations
- [ ] Use DataFrame API over RDD
- [ ] Avoid UDFs when possible
- [ ] Use built-in Spark functions
- [ ] Filter early, select late
- [ ] Repartition before expensive operations

### 3. Joins
- [ ] Broadcast small tables (<10MB)
- [ ] Use bucketing for repeated joins
- [ ] Handle data skew
- [ ] Join order optimization

### 4. Aggregations
- [ ] Use appropriate shuffle partitions
- [ ] Pre-aggregate when possible
- [ ] Use combiner functions

### 5. Memory
- [ ] Cache strategically
- [ ] Unpersist when done
- [ ] Monitor executor memory
- [ ] Tune garbage collection

### 6. I/O
- [ ] Minimize shuffle operations
- [ ] Coalesce before writing
- [ ] Use appropriate file formats
- [ ] Enable compression

---

## 💡 Common Pitfalls & Solutions

### Pitfall 1: Using collect() on Large Datasets
❌ **Problem**:
```python
df.collect()  # OOM error on large data
```

✅ **Solution**:
```python
df.take(100)  # Get sample
df.show(20)   # Preview data
df.write.parquet("output")  # Write to storage
```

### Pitfall 2: Creating Too Many Small Files
❌ **Problem**:
```python
df.write.parquet("output")  # 1000s of small files
```

✅ **Solution**:
```python
df.coalesce(10).write.parquet("output")  # Controlled file count
```

### Pitfall 3: Not Handling Nulls
❌ **Problem**:
```python
df.filter(col("age") > 18)  # Nulls cause issues
```

✅ **Solution**:
```python
df.filter((col("age") > 18) & col("age").isNotNull())
```

### Pitfall 4: Unnecessary Shuffles
❌ **Problem**:
```python
df.repartition(100).filter(...)  # Shuffle before filter
```

✅ **Solution**:
```python
df.filter(...).repartition(100)  # Filter first
```

### Pitfall 5: Using UDFs Instead of Built-ins
❌ **Problem**:
```python
@udf
def upper_case(s):
    return s.upper()
df.withColumn("name", upper_case("name"))
```

✅ **Solution**:
```python
df.withColumn("name", upper("name"))  # Built-in function
```

---

## 📊 Performance Comparison Examples

### Built-in Functions vs UDFs

```python
# Scenario: Convert string to uppercase
# Dataset: 10 million rows

Method 1: UDF
- Time: 45 seconds
- Reason: Row-by-row processing, serialization overhead

Method 2: Built-in (upper())
- Time: 8 seconds
- Reason: Optimized, vectorized operations

Speed-up: 5.6x faster
```

### Cache Storage Levels

```python
# Scenario: Iterative processing on 1GB dataset

MEMORY_ONLY:
- Time: 2 seconds per iteration
- Memory: 1GB RAM
- Best for: Enough memory available

MEMORY_AND_DISK:
- Time: 3 seconds per iteration
- Memory: 500MB RAM + 500MB disk
- Best for: Limited memory

DISK_ONLY:
- Time: 5 seconds per iteration
- Memory: 100MB RAM
- Best for: Very limited memory
```

---

## 🎤 Interview Preparation

### Common Interview Questions (with Answers)

See detailed answers in: `docs/interview_prep/common_questions.md`

1. What is the difference between transformation and action in Spark?
2. Explain lazy evaluation in Spark
3. When would you use RDD instead of DataFrame?
4. How do you optimize a slow Spark job?
5. Explain the different types of joins in Spark
6. What is the difference between cache() and persist()?
7. How do you handle data skew?
8. Explain the Catalyst optimizer
9. What is Adaptive Query Execution (AQE)?
10. How do you troubleshoot OOM errors in Spark?

### Coding Challenges

See: `exercises/` directory

- Word count variations
- Top-K problems
- Window function exercises
- Join optimization scenarios
- Data quality validation
- Real-time streaming problems

---

## 📁 Project Structure

```
pyspark_learning_path/
├── notebooks/
│   ├── 01_beginner/
│   │   ├── 01_intro_to_spark.ipynb
│   │   ├── 02_rdd_basics.ipynb
│   │   ├── 03_basic_operations.ipynb
│   │   └── 04_transformations_actions.ipynb
│   ├── 02_intermediate/
│   │   ├── 01_dataframes_intro.ipynb
│   │   ├── 02_data_sources.ipynb
│   │   ├── 03_transformations.ipynb
│   │   └── 04_joins_aggregations.ipynb
│   ├── 03_advanced/
│   │   ├── 01_window_functions.ipynb
│   │   ├── 02_udfs.ipynb
│   │   ├── 03_advanced_joins.ipynb
│   │   └── 04_partitioning.ipynb
│   └── 04_optimization/
│       ├── 01_caching.ipynb
│       ├── 02_performance_tuning.ipynb
│       ├── 03_memory_management.ipynb
│       └── 04_query_optimization.ipynb
├── examples/
│   ├── basics/
│   ├── transformations/
│   ├── actions/
│   ├── dataframes/
│   ├── sql/
│   └── streaming/
├── docs/
│   ├── theory/
│   ├── best_practices/
│   ├── trade_offs/
│   └── interview_prep/
├── exercises/
│   ├── beginner/
│   ├── intermediate/
│   ├── advanced/
│   └── solutions/
├── datasets/
├── requirements.txt
└── README.md
```

---

## 🛠️ Useful Commands

```bash
# Start Spark shell
pyspark

# Start with specific memory
pyspark --driver-memory 4g --executor-memory 4g

# Submit Spark job
spark-submit --master local[4] script.py

# Monitor Spark UI
# http://localhost:4040
```

---

## 📚 Additional Resources

### Official Documentation
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### Books
- "Learning Spark" by Jules S. Damji et al.
- "Spark: The Definitive Guide" by Bill Chambers & Matei Zaharia
- "High Performance Spark" by Holden Karau & Rachel Warren

### Online Courses
- Databricks Academy
- Coursera: Big Data Analysis with Scala and Spark
- Udemy: Apache Spark with Python

### Community
- [Stack Overflow - Apache Spark](https://stackoverflow.com/questions/tagged/apache-spark)
- [Spark User Mailing List](https://spark.apache.org/community.html)

---

## 🎯 Learning Path Roadmap

### Week 1-2: Fundamentals
- [ ] Complete beginner notebooks
- [ ] Understand RDD basics
- [ ] Practice transformations and actions
- [ ] Complete beginner exercises

### Week 3-4: DataFrames & SQL
- [ ] Master DataFrame operations
- [ ] Learn Spark SQL
- [ ] Practice joins and aggregations
- [ ] Complete intermediate exercises

### Week 5-6: Advanced Concepts
- [ ] Window functions
- [ ] UDFs and optimization
- [ ] Advanced joins
- [ ] Complete advanced exercises

### Week 7-8: Performance
- [ ] Memory management
- [ ] Query optimization
- [ ] Caching strategies
- [ ] Performance tuning

### Week 9-10: Production
- [ ] Streaming
- [ ] MLlib basics
- [ ] Deployment
- [ ] Build end-to-end project

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add your content
4. Submit a pull request

---

## 📝 License

MIT License - Feel free to use for learning and teaching

---

## 📧 Contact

For questions or suggestions:
- GitHub: [@Gowda-kiran](https://github.com/Gowda-kiran)

---

**Happy Learning! 🚀**

*Master PySpark one concept at a time. Practice makes perfect!*
