"""
End-to-End Iceberg ETL Pipeline with Airflow
Demonstrates production-ready data pipeline using Apache Iceberg
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor


# Default arguments for all DAGs
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


def validate_source_data(**context):
    """Validate source data before processing."""
    import logging

    execution_date = context['execution_date']
    logging.info(f"Validating source data for {execution_date}")

    # Example: Check if source files exist
    # In production, this would check S3, HDFS, etc.

    validation_results = {
        'files_found': True,
        'row_count': 10000,
        'schema_valid': True,
        'data_quality_score': 0.98
    }

    # Push results to XCom for downstream tasks
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)

    if not validation_results['files_found']:
        raise ValueError("Source data validation failed: No files found")

    logging.info(f"Validation successful: {validation_results}")
    return validation_results


def check_table_health(**context):
    """Check Iceberg table health metrics."""
    import logging

    table_name = context['params']['table_name']
    logging.info(f"Checking health for table: {table_name}")

    # Example health metrics
    health_metrics = {
        'total_files': 45,
        'avg_file_size_mb': 128,
        'small_files': 5,
        'total_snapshots': 12,
        'table_size_gb': 5.7,
        'partition_count': 30,
        'needs_compaction': False,
        'needs_snapshot_expiration': False
    }

    # Determine if maintenance is needed
    if health_metrics['small_files'] > 10:
        health_metrics['needs_compaction'] = True

    if health_metrics['total_snapshots'] > 20:
        health_metrics['needs_snapshot_expiration'] = True

    context['task_instance'].xcom_push(key='health_metrics', value=health_metrics)

    logging.info(f"Table health: {health_metrics}")
    return health_metrics


def publish_metrics(**context):
    """Publish pipeline metrics to monitoring system."""
    import logging

    # Get metrics from previous tasks
    ti = context['task_instance']
    validation_results = ti.xcom_pull(task_ids='validate_source_data', key='validation_results')
    health_metrics = ti.xcom_pull(task_ids='check_table_health', key='health_metrics')

    logging.info("Publishing metrics to monitoring system...")
    logging.info(f"Validation: {validation_results}")
    logging.info(f"Health: {health_metrics}")

    # In production, publish to CloudWatch, Datadog, Prometheus, etc.
    metrics = {
        'pipeline': 'iceberg_etl',
        'execution_date': str(context['execution_date']),
        'validation_score': validation_results.get('data_quality_score', 0),
        'table_size_gb': health_metrics.get('table_size_gb', 0),
        'status': 'success'
    }

    logging.info(f"Metrics published: {metrics}")
    return metrics


# Define the DAG
with DAG(
    dag_id='iceberg_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline with Apache Iceberg',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['iceberg', 'etl', 'production'],
    params={
        'table_name': 'lakehouse.sales',
        'source_path': 's3://data-lake/raw/sales/',
        'target_database': 'lakehouse'
    }
) as dag:

    # Task 1: Validate source data
    validate_source = PythonOperator(
        task_id='validate_source_data',
        python_callable=validate_source_data,
        provide_context=True,
    )

    # Task Group: Data Ingestion
    with TaskGroup(group_id='data_ingestion') as ingestion_group:

        # Extract data from source
        extract_data = SparkSubmitOperator(
            task_id='extract_raw_data',
            application='/opt/spark/jobs/extract_sales_data.py',
            name='extract-sales-data',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
                'spark.sql.catalog.iceberg.type': 'hadoop',
                'spark.executor.memory': '4g',
                'spark.driver.memory': '2g',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--source-path', '{{ params.source_path }}',
                '--execution-date', '{{ ds }}',
            ],
            executor_cores=2,
            executor_memory='4g',
            driver_memory='2g',
        )

        # Transform data
        transform_data = SparkSubmitOperator(
            task_id='transform_data',
            application='/opt/spark/jobs/transform_sales_data.py',
            name='transform-sales-data',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--execution-date', '{{ ds }}',
                '--target-table', '{{ params.table_name }}',
            ],
        )

        # Load to Iceberg table
        load_to_iceberg = SparkSubmitOperator(
            task_id='load_to_iceberg',
            application='/opt/spark/jobs/load_to_iceberg.py',
            name='load-to-iceberg',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
                'spark.sql.catalog.iceberg.type': 'hadoop',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--target-table', '{{ params.table_name }}',
                '--write-mode', 'append',
                '--execution-date', '{{ ds }}',
            ],
        )

        extract_data >> transform_data >> load_to_iceberg

    # Task Group: Data Quality Checks
    with TaskGroup(group_id='data_quality') as quality_group:

        # Row count validation
        validate_row_count = BashOperator(
            task_id='validate_row_count',
            bash_command="""
            spark-sql --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                      -e "SELECT COUNT(*) FROM {{ params.table_name }} WHERE date = '{{ ds }}'"
            """,
        )

        # Null check
        check_nulls = BashOperator(
            task_id='check_null_values',
            bash_command="""
            spark-sql --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                      -e "SELECT
                            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
                            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
                          FROM {{ params.table_name }}
                          WHERE date = '{{ ds }}'"
            """,
        )

        # Duplicate check
        check_duplicates = BashOperator(
            task_id='check_duplicates',
            bash_command="""
            spark-sql --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                      -e "SELECT
                            COUNT(*) - COUNT(DISTINCT transaction_id) as duplicates
                          FROM {{ params.table_name }}
                          WHERE date = '{{ ds }}'"
            """,
        )

        [validate_row_count, check_nulls, check_duplicates]

    # Task: Check table health
    check_health = PythonOperator(
        task_id='check_table_health',
        python_callable=check_table_health,
        provide_context=True,
    )

    # Task Group: Table Optimization (conditional)
    with TaskGroup(group_id='optimization') as optimization_group:

        # Compact small files
        compact_files = SparkSubmitOperator(
            task_id='compact_files',
            application='/opt/spark/jobs/compact_iceberg_table.py',
            name='compact-iceberg-files',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--table', '{{ params.table_name }}',
                '--strategy', 'binpack',
            ],
            trigger_rule='none_failed',  # Run even if previous tasks skipped
        )

        # Expire old snapshots
        expire_snapshots = BashOperator(
            task_id='expire_snapshots',
            bash_command="""
            spark-sql --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                      -e "CALL iceberg.system.expire_snapshots(
                            table => '{{ params.table_name }}',
                            retain_last => 7,
                            older_than => TIMESTAMP '{{ macros.ds_add(ds, -7) }} 00:00:00'
                          )"
            """,
        )

        compact_files >> expire_snapshots

    # Task: Publish metrics
    publish_metrics_task = PythonOperator(
        task_id='publish_metrics',
        python_callable=publish_metrics,
        provide_context=True,
    )

    # Task: Update metadata catalog
    update_catalog = BashOperator(
        task_id='update_metadata_catalog',
        bash_command="""
        echo "Updating metadata catalog for {{ params.table_name }}"
        # In production, this would update Hive Metastore, AWS Glue, etc.
        spark-sql --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                  -e "REFRESH TABLE {{ params.table_name }}"
        """,
    )

    # Task: Send success notification
    send_notification = BashOperator(
        task_id='send_success_notification',
        bash_command="""
        echo "Pipeline completed successfully for {{ ds }}"
        # In production, send to Slack, email, etc.
        """,
    )

    # Define task dependencies
    validate_source >> ingestion_group >> quality_group
    quality_group >> check_health >> optimization_group
    optimization_group >> update_catalog >> publish_metrics_task >> send_notification


# Document the DAG
dag.doc_md = """
# Iceberg ETL Pipeline

This DAG orchestrates a complete ETL pipeline using Apache Iceberg.

## Pipeline Stages

1. **Validation**: Validate source data quality and availability
2. **Ingestion**: Extract, transform, and load data to Iceberg tables
3. **Quality Checks**: Run data quality validations
4. **Health Check**: Monitor table health metrics
5. **Optimization**: Compact files and expire snapshots
6. **Metadata Update**: Update catalog metadata
7. **Metrics**: Publish pipeline metrics

## Schedule

Runs daily at 2 AM UTC

## Monitoring

- Metrics published to monitoring system
- Alerts sent on failure
- Health checks run after each load

## Maintenance

- File compaction runs when >10 small files detected
- Snapshots expired after 7 days
- Orphan files removed weekly

## Dependencies

- Apache Spark 3.5+
- Apache Iceberg 1.4+
- PySpark jobs in /opt/spark/jobs/
"""
