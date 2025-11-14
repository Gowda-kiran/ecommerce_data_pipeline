# E-commerce Data Pipeline

A production-ready, scalable data engineering project demonstrating ETL best practices with modern tools including **PySpark**, **Apache Airflow**, **Docker**, and comprehensive **data quality** frameworks.

## Overview

This project implements a complete end-to-end data pipeline for e-commerce analytics, following the **Medallion Architecture** (Bronze, Silver, Gold layers) and **dimensional modeling** principles.

### Key Features

- **Multi-layered Data Architecture**: Raw (Bronze) → Cleansed (Silver) → Analytics-Ready (Gold)
- **Distributed Processing**: PySpark for scalable data transformations
- **Workflow Orchestration**: Apache Airflow DAGs for automated pipeline execution
- **Data Quality**: Comprehensive validation framework with completeness, accuracy, and consistency checks
- **Dimensional Modeling**: Star schema with fact and dimension tables optimized for analytics
- **Containerization**: Docker setup for reproducible environments
- **Testing**: Unit and integration tests with pytest
- **Production-Ready**: Logging, error handling, and monitoring capabilities

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                            │
│                   (CSV Files: Transactions,                      │
│                    Customers, Products)                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (RAW)                          │
│                  - Raw data ingestion                            │
│                  - Parquet format                                │
│                  - Metadata tracking                             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (CLEANSED)                       │
│              - Data cleansing & standardization                  │
│              - Deduplication                                     │
│              - Business rule validation                          │
│              - Derived columns                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA QUALITY CHECKS                          │
│              - Completeness validation                           │
│              - Referential integrity                             │
│              - Statistical anomaly detection                     │
│              - Business rule compliance                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   GOLD LAYER (ANALYTICS)                         │
│                    Star Schema Model:                            │
│              - dim_customer                                      │
│              - dim_product                                       │
│              - dim_date                                          │
│              - fact_sales                                        │
│              - fact_sales_aggregated                             │
└─────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| **Processing Engine** | Apache Spark (PySpark 3.5.0) |
| **Orchestration** | Apache Airflow 2.8.0 |
| **Data Storage** | Parquet (with Snappy compression) |
| **Database** | PostgreSQL 13 |
| **Containerization** | Docker & Docker Compose |
| **Testing** | pytest, pytest-cov |
| **Data Quality** | Great Expectations |
| **Language** | Python 3.9+ |
| **Code Quality** | Black, Flake8, Pylint |

## Project Structure

```
ecommerce_data_pipeline/
├── config/                     # Configuration files
│   └── config.yaml            # Pipeline configuration
├── dags/                       # Airflow DAGs
│   └── ecommerce_etl_dag.py   # Main ETL orchestration DAG
├── data/                       # Data directories
│   ├── raw/                   # Raw source data (Bronze)
│   ├── bronze/                # Ingested data
│   ├── silver/                # Cleansed data
│   └── gold/                  # Analytics-ready data
├── docker/                     # Docker configurations
├── docs/                       # Documentation
├── sql/                        # SQL scripts
│   ├── ddl/                   # Schema definitions
│   │   └── create_schema.sql
│   └── dml/                   # Sample queries
│       └── sample_queries.sql
├── src/                        # Source code
│   ├── extraction/            # Data extraction modules
│   │   └── data_extractor.py
│   ├── transformation/        # Data transformation modules
│   │   └── data_transformer.py
│   ├── loading/               # Data loading modules
│   │   └── dimensional_model.py
│   ├── quality/               # Data quality modules
│   │   └── data_quality_checks.py
│   └── utils/                 # Utility modules
│       ├── data_generator.py  # Sample data generator
│       └── spark_utils.py     # Spark session manager
├── tests/                      # Test suite
│   ├── unit/                  # Unit tests
│   └── integration/           # Integration tests
├── .env.example               # Environment variables template
├── .dockerignore              # Docker ignore file
├── docker-compose.yml         # Docker Compose configuration
├── Dockerfile                 # Docker image definition
├── Makefile                   # Common commands
├── pytest.ini                 # Pytest configuration
├── requirements.txt           # Python dependencies
├── setup.py                   # Package setup
└── README.md                  # This file
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Make (optional, for convenience)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Gowda-kiran/ecommerce_data_pipeline.git
   cd ecommerce_data_pipeline
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   ```

3. **Install dependencies (local development)**
   ```bash
   make install
   # OR
   pip install -r requirements.txt
   ```

### Running the Pipeline

#### Option 1: Using Docker (Recommended)

```bash
# Build and start all services
make docker-up
# OR
docker-compose up -d

# Access Airflow UI at http://localhost:8080
# Username: airflow
# Password: airflow
```

#### Option 2: Local Execution

```bash
# Generate sample data
make generate-data

# Run complete pipeline
make run-pipeline

# Or run individual steps:
python -m src.extraction.data_extractor
python -m src.transformation.data_transformer
python -m src.loading.dimensional_model
```

## Data Pipeline Workflow

### 1. Data Generation
```bash
python src/utils/data_generator.py
```
Generates realistic e-commerce data:
- **10,000 customers** with demographics
- **500 products** across multiple categories
- **50,000 transactions** with line items

### 2. Extraction (Bronze Layer)
- Reads raw CSV files
- Adds metadata (ingestion timestamp, source file)
- Writes to Parquet format
- Implements data lineage tracking

### 3. Transformation (Silver Layer)
- **Cleansing**: Removes nulls, trims whitespace, standardizes formats
- **Enrichment**: Adds derived columns (customer tenure, profit margins, etc.)
- **Validation**: Applies business rules and data quality checks
- **Deduplication**: Removes duplicate records
- **Partitioning**: Optimizes storage with partition pruning

### 4. Data Quality Validation
- **Completeness**: Ensures required fields are populated (95% threshold)
- **Uniqueness**: Detects duplicate records (max 1% duplicates)
- **Referential Integrity**: Validates foreign key relationships
- **Range Validation**: Checks numeric values within expected bounds
- **Anomaly Detection**: Statistical outlier detection (3-sigma rule)

### 5. Dimensional Modeling (Gold Layer)

**Star Schema Design:**

#### Dimension Tables
- `dim_customer`: Customer attributes with SCD Type 2 support
- `dim_product`: Product catalog with pricing and inventory
- `dim_date`: Date dimension for time-series analysis

#### Fact Tables
- `fact_sales`: Transactional grain (one row per transaction)
- `fact_sales_aggregated`: Pre-aggregated daily summaries

## Configuration

All pipeline configurations are in `config/config.yaml`:

```yaml
paths:
  raw_data: "data/raw"
  bronze_layer: "data/bronze"
  silver_layer: "data/silver"
  gold_layer: "data/gold"

quality:
  min_completeness: 0.95
  max_duplicate_rate: 0.01
  max_anomaly_score: 0.05

business_rules:
  min_order_amount: 0.01
  max_order_amount: 1000000
  valid_payment_methods: ["credit_card", "debit_card", "paypal", "wallet"]
```

## Testing

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test file
pytest tests/unit/test_transformations.py -v
```

## Code Quality

```bash
# Format code
make format

# Run linting
make lint

# Clean up artifacts
make clean
```

## Airflow DAG

The pipeline is orchestrated by Airflow with the following task dependencies:

```
extract_data → transform_data → quality_validation → build_dimensional_model → cleanup
```

**Schedule**: Daily at 2:00 AM UTC
**Retries**: 3 attempts with 5-minute delay
**Timeout**: 2 hours per task

## Sample Analytics Queries

Explore pre-built analytics queries in `sql/dml/sample_queries.sql`:

1. Top 10 products by revenue
2. Monthly sales trends
3. Customer segmentation analysis
4. Product category performance
5. Weekend vs weekday sales
6. Top customers by lifetime value
7. Payment method analysis
8. Hourly sales patterns
9. Product stock analysis
10. Customer acquisition trends

## Key Metrics & KPIs

The pipeline enables analysis of:

- **Revenue Metrics**: Total sales, average order value, revenue by category
- **Customer Metrics**: Lifetime value, acquisition cost, retention rate
- **Product Metrics**: Best sellers, profit margins, inventory turnover
- **Operational Metrics**: Order fulfillment rate, return rate, payment methods

## Data Quality Report

After each pipeline run, a data quality report is generated with:
- Total checks performed
- Pass/fail rates
- Failed check details
- Metric values vs. thresholds

## Scalability Considerations

1. **Partitioning**: Data partitioned by year/month for query optimization
2. **Compression**: Snappy compression reduces storage by ~60%
3. **Incremental Processing**: Supports incremental loads (append mode)
4. **Distributed Processing**: Spark enables horizontal scaling
5. **Connection Pooling**: Efficient database connection management

## Monitoring & Logging

- **Structured Logging**: All modules use `structlog` for consistent logging
- **Airflow UI**: Task execution monitoring and alerting
- **Log Aggregation**: Centralized logs in `/logs` directory
- **Metrics Tracking**: Record counts, execution times, data quality scores

## Interview Discussion Points

This project demonstrates proficiency in:

1. **Data Architecture**: Multi-layered medallion architecture
2. **ETL Best Practices**: Separation of concerns, idempotency, error handling
3. **Data Modeling**: Dimensional modeling, star schema, SCD Type 2
4. **Data Quality**: Comprehensive validation framework
5. **Orchestration**: DAG design, task dependencies, retry logic
6. **Testing**: Unit tests, integration tests, code coverage
7. **DevOps**: Docker containerization, CI/CD readiness
8. **Performance**: Partitioning, compression, distributed processing
9. **Code Quality**: PEP 8 compliance, documentation, type hints
10. **Production Readiness**: Logging, monitoring, configuration management

## Common Commands

```bash
# Development
make install          # Install dependencies
make generate-data    # Create sample data
make run-pipeline     # Execute pipeline locally
make test            # Run tests

# Docker
make docker-build    # Build Docker images
make docker-up       # Start containers
make docker-down     # Stop containers
make docker-logs     # View logs

# Code Quality
make format          # Format code with Black
make lint            # Run linters
make clean           # Remove artifacts
```

## Troubleshooting

### Issue: Airflow webserver not starting
**Solution**: Check logs with `docker-compose logs airflow-webserver`

### Issue: Spark job fails with memory error
**Solution**: Increase memory in `config.yaml` spark configs

### Issue: Data quality checks failing
**Solution**: Review thresholds in `config/config.yaml`

## Future Enhancements

- [ ] Add streaming data ingestion (Kafka integration)
- [ ] Implement CDC (Change Data Capture)
- [ ] Add machine learning predictions
- [ ] Create dashboards (Tableau/PowerBI integration)
- [ ] Implement data catalog (DataHub/Amundsen)
- [ ] Add data lineage visualization
- [ ] Implement data versioning (Delta Lake)
- [ ] Add alerting (Slack/PagerDuty integration)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Contact

For questions or feedback:
- GitHub: [@Gowda-kiran](https://github.com/Gowda-kiran)
- Project: [ecommerce_data_pipeline](https://github.com/Gowda-kiran/ecommerce_data_pipeline)

---

**Built with best practices for Amazon and American Express data engineering interviews**
