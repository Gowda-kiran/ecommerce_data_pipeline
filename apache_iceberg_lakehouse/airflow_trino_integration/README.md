# Apache Iceberg with Airflow and Trino Integration

Comprehensive examples of building production data pipelines using Apache Iceberg, Apache Airflow, and Trino.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Airflow DAGs](#airflow-dags)
- [Trino Queries](#trino-queries)
- [Setup](#setup)
- [Best Practices](#best-practices)
- [Monitoring](#monitoring)

---

## 🎯 Overview

This integration demonstrates enterprise-grade data engineering patterns combining:

- **Apache Iceberg**: Modern table format with ACID transactions, schema evolution, time travel
- **Apache Airflow**: Workflow orchestration and scheduling
- **Trino**: Distributed SQL query engine for analytics

### Key Features

✅ **Real-Time CDC Ingestion** - Capture changes from source databases
✅ **Automated Table Maintenance** - Compaction, snapshot management, optimization
✅ **Time Travel Analytics** - Query historical data states
✅ **Data Quality Framework** - Automated validation and monitoring
✅ **SLA Monitoring** - Track pipeline performance and compliance
✅ **Auto-Remediation** - Automatic issue resolution

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
│  MySQL, PostgreSQL, Kafka, S3, APIs, Database Logs          │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│              AIRFLOW ORCHESTRATION                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  ETL DAGs    │  │  CDC DAGs    │  │ Maintenance  │      │
│  │              │  │              │  │    DAGs      │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│               APACHE SPARK PROCESSING                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Extract    │  │  Transform   │  │     Load     │      │
│  │              │  │              │  │              │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│              APACHE ICEBERG LAKEHOUSE                        │
│  ┌────────────────────────────────────────────────────┐     │
│  │  Tables: sales, customers, products, orders        │     │
│  │  Features: ACID, Time Travel, Schema Evolution     │     │
│  │  Storage: S3, HDFS, Cloud Storage                  │     │
│  └────────────────────────────────────────────────────┘     │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                  TRINO QUERY ENGINE                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Analytics   │  │  Reporting   │  │   Ad-hoc     │      │
│  │   Queries    │  │    Queries   │  │   Queries    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│         ANALYTICS & VISUALIZATION                            │
│  Tableau, PowerBI, Jupyter, Superset, Redash               │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 Components

### Airflow DAGs

Located in `airflow/dags/`

#### 1. **iceberg_etl_pipeline.py**
End-to-end ETL pipeline with Iceberg

**Features:**
- Source data validation
- Spark-based extraction, transformation, loading
- Data quality checks (null, duplicates, row counts)
- Table health monitoring
- Automatic compaction and optimization
- Metrics publishing

**Schedule:** Daily at 2 AM UTC

**Key Tasks:**
```python
validate_source → extract_data → transform_data → load_to_iceberg
  → quality_checks → health_check → optimization → publish_metrics
```

#### 2. **iceberg_table_maintenance.py**
Automated table maintenance and optimization

**Features:**
- File compaction (binpack strategy)
- Snapshot expiration
- Orphan file removal
- Manifest rewrite
- Health-based decision making

**Schedule:** Weekly on Sunday at 3 AM

**Optimization Operations:**
```sql
-- Compact small files
CALL iceberg.system.rewrite_data_files(
    table => 'lakehouse.sales',
    strategy => 'binpack'
)

-- Expire old snapshots
CALL iceberg.system.expire_snapshots(
    table => 'lakehouse.sales',
    retain_last => 7
)

-- Remove orphan files
CALL iceberg.system.remove_orphan_files(
    table => 'lakehouse.sales'
)
```

#### 3. **iceberg_cdc_ingestion.py**
Real-time CDC ingestion from source databases

**Features:**
- Debezium/DMS CDC event processing
- MERGE-based upserts
- Exactly-once semantics
- SLA monitoring (< 15 min lag)
- Checkpoint management

**Schedule:** Every 15 minutes

**CDC Merge Pattern:**
```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

#### 4. **iceberg_monitoring_alerts.py**
Comprehensive monitoring and alerting

**Features:**
- Table health metrics collection
- SLA compliance tracking
- Alert generation (critical/warning)
- Metrics export to monitoring systems
- Dashboard updates

**Schedule:** Hourly

**Monitored Metrics:**
- File counts and sizes
- Small file ratio
- Snapshot count
- Data freshness
- SLA violations

---

### Trino Integration

Located in `trino/`

#### data_modeling_examples.sql

Comprehensive SQL examples demonstrating:

**1. Time Travel**
```sql
-- Query as of timestamp
SELECT * FROM lakehouse.sales
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:00:00'

-- Query as of snapshot
SELECT * FROM lakehouse.sales
FOR VERSION AS OF 1234567890
```

**2. Metadata Queries**
```sql
-- View snapshots
SELECT * FROM lakehouse."sales$snapshots"

-- View files
SELECT * FROM lakehouse."sales$files"

-- Partition statistics
SELECT partition, COUNT(*) as file_count
FROM lakehouse."sales$files"
GROUP BY partition
```

**3. Analytical Queries**
- Customer Lifetime Value (CLV)
- Cohort Analysis
- RFM Segmentation
- Market Basket Analysis
- Sales Trends with Moving Averages

**4. Data Quality**
- Null checks
- Duplicate detection
- Freshness validation
- Outlier detection

#### trino_iceberg_client.py

Python client for programmatic Trino queries

**Features:**
```python
from trino_iceberg_client import TrinoIcebergClient

client = TrinoIcebergClient(host='localhost', catalog='iceberg')

# Get table snapshots
snapshots = client.get_table_snapshots('sales')

# Time travel query
historical = client.time_travel_query(
    table='sales',
    timestamp=datetime(2024, 1, 15)
)

# Health metrics
health = client.get_table_health_metrics('sales')

# Data quality checks
quality = client.run_data_quality_checks('sales')
```

---

## 🚀 Setup

### Prerequisites

```bash
# Airflow
pip install apache-airflow==2.8.0
pip install apache-airflow-providers-apache-spark

# Trino
pip install trino==0.328.0

# Spark with Iceberg
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2
```

### Configuration

#### 1. Airflow Configuration

**airflow.cfg**
```ini
[core]
dags_folder = /path/to/airflow/dags
executor = LocalExecutor

[scheduler]
dag_dir_list_interval = 30
```

**Connection Setup**
```bash
# Add Spark connection
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://localhost \
    --conn-port 7077

# Add Trino connection
airflow connections add trino_default \
    --conn-type trino \
    --conn-host localhost \
    --conn-port 8080 \
    --conn-schema lakehouse
```

#### 2. Spark Configuration

**spark-defaults.conf**
```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=hadoop
spark.sql.catalog.iceberg.warehouse=/data/iceberg_warehouse
```

#### 3. Trino Configuration

**catalog/iceberg.properties**
```properties
connector.name=iceberg
iceberg.catalog.type=hadoop
iceberg.catalog.warehouse=/data/iceberg_warehouse
```

---

## 📚 Best Practices

### 1. Table Design

**Partitioning Strategy:**
```sql
-- Good: Coarse partitioning
PARTITIONED BY (months(order_date), region)

-- Bad: Too granular
PARTITIONED BY (hours(order_date))
```

**Target File Sizes:**
- Development: 128 MB
- Production: 512 MB - 1 GB
- Avoid: < 10 MB (too small), > 5 GB (too large)

### 2. CDC Pipeline Design

**Idempotency:**
- Use MERGE for upserts
- Implement checkpoint-based recovery
- Handle late-arriving data

**Performance:**
- Process in micro-batches (15 min intervals)
- Use partition pruning
- Enable predicate pushdown

### 3. Maintenance Schedule

**Daily:**
- Monitor file sizes
- Check SLA compliance
- Review query performance

**Weekly:**
- Run file compaction
- Expire snapshots (7-14 day retention)
- Analyze partition distribution

**Monthly:**
- Remove orphan files
- Full table optimization
- Review partition strategy

### 4. Query Optimization

**Use Partition Filters:**
```sql
-- Good
WHERE order_date = DATE '2024-01-15'

-- Bad
WHERE DATE_FORMAT(order_date, '%Y-%m-%d') = '2024-01-15'
```

**Column Projection:**
```sql
-- Good: Select only needed columns
SELECT customer_id, amount FROM sales

-- Bad: Select all columns
SELECT * FROM sales
```

### 5. Monitoring

**Key Metrics to Track:**
- Pipeline lag (target: < 15 min)
- Query latency (p95, p99)
- File count and sizes
- Snapshot count
- Data quality scores

**Alert Thresholds:**
- Critical: Data lag > 1 hour, SLA violation
- Warning: Small file ratio > 30%, Snapshot count > 30
- Info: General health metrics

---

## 📊 Monitoring

### Metrics Dashboard

Track these metrics in Grafana/CloudWatch:

**Pipeline Metrics:**
```
- iceberg_pipeline_duration_seconds
- iceberg_pipeline_success_rate
- iceberg_records_processed_total
- iceberg_pipeline_lag_seconds
```

**Table Metrics:**
```
- iceberg_table_size_bytes
- iceberg_table_file_count
- iceberg_table_small_file_ratio
- iceberg_table_snapshot_count
```

**Query Metrics:**
```
- trino_query_duration_seconds
- trino_query_rows_scanned
- trino_query_success_rate
```

### Example Grafana Queries

**Pipeline Success Rate:**
```promql
sum(rate(iceberg_pipeline_success_total[5m]))
/
sum(rate(iceberg_pipeline_total[5m]))
```

**Average Pipeline Lag:**
```promql
avg(iceberg_pipeline_lag_seconds) by (table)
```

---

## 🎓 Learning Path

### Building Pipelines with Iceberg and Airflow (8 hours)

**Module 1: Airflow Fundamentals (2 hours)**
- DAG design patterns
- Task dependencies
- Sensors and operators
- Error handling and retries

**Module 2: Iceberg Integration (3 hours)**
- Spark Submit operators
- Table operations
- MERGE upserts
- Maintenance procedures

**Module 3: Production Patterns (3 hours)**
- CDC ingestion
- Data quality checks
- Monitoring and alerting
- Auto-remediation

### Data Modeling with Iceberg and Trino (3.9 hours)

**Module 1: Trino Basics (1 hour)**
- Catalog setup
- Query execution
- Performance tuning

**Module 2: Iceberg Metadata (1.4 hours)**
- Snapshot queries
- File statistics
- Time travel

**Module 3: Analytics (1.5 hours)**
- Dimensional modeling
- Analytical queries
- Window functions

### Airflow Pipelines with Iceberg (4.1 hours)

**Module 1: ETL Pipelines (1.5 hours)**
- End-to-end workflows
- Task groups
- Dynamic DAGs

**Module 2: CDC Pipelines (1.5 hours)**
- Real-time ingestion
- MERGE operations
- Checkpoint management

**Module 3: Monitoring (1.1 hours)**
- Health checks
- SLA tracking
- Alert configuration

---

## 📝 Examples

### Run ETL Pipeline

```bash
# Trigger DAG
airflow dags trigger iceberg_etl_pipeline \
    --conf '{"table_name": "lakehouse.sales"}'

# Monitor execution
airflow dags list-runs -d iceberg_etl_pipeline
```

### Execute Trino Query

```bash
# Via CLI
trino --catalog iceberg --schema lakehouse \
    --execute "SELECT COUNT(*) FROM sales"

# Via Python
from trino_iceberg_client import TrinoIcebergClient

client = TrinoIcebergClient()
df = client.execute_query("SELECT * FROM sales LIMIT 10")
print(df)
```

### Manual Maintenance

```bash
# Trigger maintenance DAG
airflow dags trigger iceberg_table_maintenance \
    --conf '{"table_name": "lakehouse.sales"}'
```

---

## 🔍 Troubleshooting

### Common Issues

**Issue: High CDC Lag**
- Check Kafka/source lag
- Increase processing frequency
- Optimize MERGE queries

**Issue: Small File Problem**
- Run file compaction
- Adjust write batch sizes
- Review partition strategy

**Issue: Slow Queries**
- Check partition pruning
- Review predicate pushdown
- Analyze query plans

---

## 📖 References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Apache Airflow Documentation](https://airflow.apache.org/)
- [Trino Documentation](https://trino.io/docs/)

---

## 🎯 Summary

This integration provides production-ready patterns for:

✅ **8 hrs** - Building robust Airflow pipelines with Iceberg
✅ **3.9 hrs** - Data modeling and analytics with Trino
✅ **4.1 hrs** - Production CDC and ETL workflows

**Total:** 16 hours of comprehensive examples and best practices for enterprise data engineering with Iceberg, Airflow, and Trino.
