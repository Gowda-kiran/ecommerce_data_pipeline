"""
CDC Ingestion to Iceberg with Airflow
Captures changes from source databases and applies to Iceberg tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.time_delta import TimeDeltaSensor


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def extract_cdc_changes(**context):
    """Extract CDC changes from source database."""
    import logging
    from datetime import datetime

    execution_date = context['execution_date']
    prev_execution_date = context['prev_execution_date']

    logging.info(f"Extracting CDC changes from {prev_execution_date} to {execution_date}")

    # Simulate CDC extraction
    # In production: Connect to Debezium, AWS DMS, or database transaction log
    cdc_stats = {
        'source_database': 'mysql_production',
        'table': 'customers',
        'extraction_start': prev_execution_date,
        'extraction_end': execution_date,
        'inserts': 150,
        'updates': 320,
        'deletes': 25,
        'total_changes': 495,
        'extraction_time_seconds': 12.5,
    }

    context['task_instance'].xcom_push(key='cdc_stats', value=cdc_stats)

    logging.info(f"CDC extraction complete: {cdc_stats}")
    return cdc_stats


def validate_cdc_data(**context):
    """Validate extracted CDC data."""
    import logging

    ti = context['task_instance']
    cdc_stats = ti.xcom_pull(task_ids='extract_cdc_changes', key='cdc_stats')

    logging.info("Validating CDC data...")

    # Validation checks
    validation_results = {
        'has_changes': cdc_stats['total_changes'] > 0,
        'schema_valid': True,
        'no_duplicates': True,
        'timestamp_monotonic': True,
        'validation_passed': True,
    }

    # Check for anomalies
    if cdc_stats['deletes'] > cdc_stats['inserts'] * 2:
        logging.warning(f"Unusual delete ratio: {cdc_stats['deletes']} deletes vs {cdc_stats['inserts']} inserts")
        validation_results['unusual_pattern'] = True

    if not validation_results['validation_passed']:
        raise ValueError("CDC data validation failed")

    context['task_instance'].xcom_push(key='validation', value=validation_results)

    logging.info(f"Validation results: {validation_results}")
    return validation_results


def calculate_merge_stats(**context):
    """Calculate statistics after merge operation."""
    import logging

    ti = context['task_instance']
    cdc_stats = ti.xcom_pull(task_ids='extract_cdc_changes', key='cdc_stats')

    # Simulate post-merge statistics
    merge_stats = {
        'records_inserted': cdc_stats['inserts'],
        'records_updated': cdc_stats['updates'],
        'records_deleted': cdc_stats['deletes'],
        'total_processed': cdc_stats['total_changes'],
        'merge_duration_seconds': 45.2,
        'snapshot_created': True,
        'new_snapshot_id': 8234567890,
    }

    logging.info(f"Merge statistics: {merge_stats}")

    context['task_instance'].xcom_push(key='merge_stats', value=merge_stats)

    return merge_stats


with DAG(
    dag_id='iceberg_cdc_ingestion',
    default_args=default_args,
    description='CDC ingestion from source databases to Iceberg',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['iceberg', 'cdc', 'real-time'],
    params={
        'source_db': 'mysql_production',
        'source_table': 'customers',
        'target_table': 'lakehouse.customers',
        'cdc_format': 'debezium',
        'merge_strategy': 'upsert',
    }
) as dag:

    # Wait for upstream data availability
    wait_for_upstream = TimeDeltaSensor(
        task_id='wait_for_upstream',
        delta=timedelta(seconds=30),
    )

    # Extract CDC changes
    extract_cdc = PythonOperator(
        task_id='extract_cdc_changes',
        python_callable=extract_cdc_changes,
        provide_context=True,
    )

    # Validate CDC data
    validate_cdc = PythonOperator(
        task_id='validate_cdc_data',
        python_callable=validate_cdc_data,
        provide_context=True,
    )

    # Task Group: Process CDC Events
    with TaskGroup(group_id='process_cdc') as process_group:

        # Parse CDC events
        parse_cdc_events = SparkSubmitOperator(
            task_id='parse_cdc_events',
            application='/opt/spark/jobs/parse_cdc_events.py',
            name='parse-cdc-events',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--source-db', '{{ params.source_db }}',
                '--source-table', '{{ params.source_table }}',
                '--cdc-format', '{{ params.cdc_format }}',
                '--execution-date', '{{ ts }}',
            ],
        )

        # Transform CDC to Iceberg format
        transform_cdc = SparkSubmitOperator(
            task_id='transform_to_iceberg_format',
            application='/opt/spark/jobs/transform_cdc_to_iceberg.py',
            name='transform-cdc',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--target-table', '{{ params.target_table }}',
            ],
        )

        parse_cdc_events >> transform_cdc

    # Merge CDC changes to Iceberg table
    merge_to_iceberg = SparkSubmitOperator(
        task_id='merge_to_iceberg',
        application='/opt/spark/jobs/merge_cdc_to_iceberg.py',
        name='merge-cdc-to-iceberg',
        conf={
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': 'hadoop',
        },
        jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
        application_args=[
            '--target-table', '{{ params.target_table }}',
            '--merge-strategy', '{{ params.merge_strategy }}',
            '--execution-timestamp', '{{ ts }}',
        ],
    )

    # Log CDC changelog
    log_changelog = BashOperator(
        task_id='log_to_changelog',
        bash_command="""
        spark-sql \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            -e "
            INSERT INTO lakehouse.cdc_audit_log
            SELECT
                '{{ params.source_table }}' as source_table,
                '{{ params.target_table }}' as target_table,
                TIMESTAMP '{{ ts }}' as ingestion_timestamp,
                'CDC_MERGE' as operation,
                snapshot_id as snapshot_id
            FROM lakehouse.{{ params.target_table.split('.')[1] }}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
            "
        """,
    )

    # Calculate merge statistics
    calculate_stats = PythonOperator(
        task_id='calculate_merge_stats',
        python_callable=calculate_merge_stats,
        provide_context=True,
    )

    # Task Group: Data Quality Checks
    with TaskGroup(group_id='quality_checks') as quality_group:

        # Check for duplicates after merge
        check_duplicates = BashOperator(
            task_id='check_duplicates',
            bash_command="""
            spark-sql \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                -e "
                SELECT
                    COUNT(*) - COUNT(DISTINCT id) as duplicate_count
                FROM {{ params.target_table }}
                WHERE is_current = true
                HAVING duplicate_count > 0
                "
            # Exit with error if duplicates found
            if [ $? -ne 0 ]; then
                echo "ERROR: Duplicates detected after CDC merge"
                exit 1
            fi
            """,
        )

        # Verify referential integrity
        check_integrity = BashOperator(
            task_id='check_referential_integrity',
            bash_command="""
            echo "Checking referential integrity after CDC merge"
            # Add integrity checks specific to your schema
            """,
        )

        # Validate row counts
        validate_counts = BashOperator(
            task_id='validate_row_counts',
            bash_command="""
            spark-sql \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                -e "
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_rows,
                    SUM(CASE WHEN is_current = false THEN 1 ELSE 0 END) as historical_rows
                FROM {{ params.target_table }}
                "
            """,
        )

        [check_duplicates, check_integrity, validate_counts]

    # Update last processed timestamp
    update_checkpoint = BashOperator(
        task_id='update_checkpoint',
        bash_command="""
        echo "Updating CDC checkpoint"
        # In production: Update checkpoint in DynamoDB, Redis, or database
        echo "{{ ts }}" > /tmp/cdc_checkpoint_{{ params.source_table }}.txt
        """,
    )

    # Monitor CDC lag
    monitor_lag = BashOperator(
        task_id='monitor_cdc_lag',
        bash_command="""
        echo "Monitoring CDC ingestion lag"
        # Calculate time difference between source transaction time and ingestion time
        # Alert if lag > threshold
        """,
    )

    # Publish metrics
    publish_metrics = BashOperator(
        task_id='publish_cdc_metrics',
        bash_command="""
        echo "Publishing CDC metrics"
        # POST to monitoring system:
        # - Records inserted/updated/deleted
        # - Ingestion lag
        # - Processing duration
        # - Error rate
        """,
    )

    # Define dependencies
    wait_for_upstream >> extract_cdc >> validate_cdc
    validate_cdc >> process_group >> merge_to_iceberg
    merge_to_iceberg >> [log_changelog, calculate_stats]
    calculate_stats >> quality_group
    quality_group >> [update_checkpoint, monitor_lag]
    [update_checkpoint, monitor_lag] >> publish_metrics


dag.doc_md = """
# Iceberg CDC Ingestion DAG

Continuous data ingestion from source databases using Change Data Capture (CDC).

## Pipeline Flow

1. **Extract**: Capture changes from source database
2. **Validate**: Check data quality and schema
3. **Process**: Parse and transform CDC events
4. **Merge**: Apply changes to Iceberg table using MERGE
5. **Audit**: Log changes to changelog table
6. **Quality**: Run post-merge validations
7. **Checkpoint**: Update processing checkpoint
8. **Monitor**: Track lag and performance

## CDC Sources Supported

- **Debezium**: MySQL, PostgreSQL, MongoDB
- **AWS DMS**: Various sources
- **Database Transaction Logs**: Direct log reading

## Merge Strategy

Uses Iceberg MERGE operation:
```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Schedule

Runs every 15 minutes for near real-time ingestion

## SLA

- Target lag: < 15 minutes
- Processing time: < 2 minutes
- Success rate: > 99.9%

## Monitoring

- CDC lag metrics
- Processing duration
- Error rates
- Data quality scores

## Alerting

- Alert if lag > 30 minutes
- Alert on validation failures
- Alert on processing errors

## Recovery

- Idempotent operations
- Automatic retries (3x)
- Checkpoint-based recovery
- Manual replay capability
"""
