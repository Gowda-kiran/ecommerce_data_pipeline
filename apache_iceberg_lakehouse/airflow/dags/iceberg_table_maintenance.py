"""
Iceberg Table Maintenance DAG
Automates compaction, snapshot management, and optimization
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'data-ops',
    'depends_on_past': False,
    'email': ['data-ops@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def analyze_table_health(**context):
    """Analyze table and determine maintenance needs."""
    import logging

    table_name = context['params']['table_name']
    logging.info(f"Analyzing table: {table_name}")

    # Simulate table analysis
    # In production, query Iceberg metadata tables
    analysis = {
        'table': table_name,
        'total_files': 150,
        'small_files': 45,  # files < 10 MB
        'avg_file_size_mb': 85,
        'total_snapshots': 25,
        'oldest_snapshot_days': 15,
        'orphan_files': 12,
        'needs_compaction': True,
        'needs_snapshot_expiry': True,
        'needs_orphan_cleanup': True,
    }

    # Determine if compaction is needed
    small_file_threshold = 0.3  # 30% small files
    if analysis['small_files'] / analysis['total_files'] > small_file_threshold:
        analysis['needs_compaction'] = True

    # Determine if snapshot expiration is needed
    if analysis['total_snapshots'] > 20 or analysis['oldest_snapshot_days'] > 14:
        analysis['needs_snapshot_expiry'] = True

    logging.info(f"Analysis results: {analysis}")
    context['task_instance'].xcom_push(key='analysis', value=analysis)

    return analysis


def decide_maintenance_tasks(**context):
    """Decide which maintenance tasks to run based on analysis."""
    ti = context['task_instance']
    analysis = ti.xcom_pull(task_ids='analyze_table', key='analysis')

    tasks_to_run = []

    if analysis.get('needs_compaction', False):
        tasks_to_run.append('maintenance.compact_small_files')

    if analysis.get('needs_snapshot_expiry', False):
        tasks_to_run.append('maintenance.expire_snapshots')

    if analysis.get('needs_orphan_cleanup', False):
        tasks_to_run.append('maintenance.remove_orphan_files')

    # If no maintenance needed, skip to metrics
    if not tasks_to_run:
        tasks_to_run.append('skip_maintenance')

    return tasks_to_run


def calculate_maintenance_stats(**context):
    """Calculate and report maintenance statistics."""
    import logging

    ti = context['task_instance']
    analysis_before = ti.xcom_pull(task_ids='analyze_table', key='analysis')

    # Simulate post-maintenance analysis
    analysis_after = {
        'total_files': 75,  # Reduced from compaction
        'small_files': 5,
        'avg_file_size_mb': 165,
        'total_snapshots': 7,  # Reduced from expiration
        'orphan_files': 0,
    }

    stats = {
        'files_reduced': analysis_before['total_files'] - analysis_after['total_files'],
        'snapshots_expired': analysis_before['total_snapshots'] - analysis_after['total_snapshots'],
        'orphans_removed': analysis_before['orphan_files'],
        'storage_saved_gb': 2.5,
        'maintenance_duration_minutes': 15,
    }

    logging.info(f"Maintenance statistics: {stats}")

    # Push to monitoring system
    context['task_instance'].xcom_push(key='maintenance_stats', value=stats)

    return stats


with DAG(
    dag_id='iceberg_table_maintenance',
    default_args=default_args,
    description='Automated Iceberg table maintenance and optimization',
    schedule_interval='0 3 * * 0',  # Weekly on Sunday at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['iceberg', 'maintenance', 'optimization'],
    params={
        'table_name': 'lakehouse.sales',
        'compaction_strategy': 'binpack',
        'snapshot_retention_days': 7,
        'target_file_size_mb': 512,
    }
) as dag:

    # Analyze table health
    analyze_table = PythonOperator(
        task_id='analyze_table',
        python_callable=analyze_table_health,
        provide_context=True,
    )

    # Decide which maintenance tasks to run
    decide_tasks = BranchPythonOperator(
        task_id='decide_maintenance_tasks',
        python_callable=decide_maintenance_tasks,
        provide_context=True,
    )

    # Maintenance task group
    with TaskGroup(group_id='maintenance') as maintenance_group:

        # Compact small files
        compact_small_files = SparkSubmitOperator(
            task_id='compact_small_files',
            application='/opt/spark/jobs/iceberg_compaction.py',
            name='iceberg-file-compaction',
            conf={
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
                'spark.sql.catalog.iceberg.type': 'hadoop',
                'spark.executor.memory': '8g',
                'spark.driver.memory': '4g',
            },
            jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            application_args=[
                '--table', '{{ params.table_name }}',
                '--strategy', '{{ params.compaction_strategy }}',
                '--target-file-size-mb', '{{ params.target_file_size_mb }}',
            ],
            executor_cores=4,
            num_executors=4,
        )

        # Expire old snapshots
        expire_snapshots = BashOperator(
            task_id='expire_snapshots',
            bash_command="""
            spark-sql \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                --conf spark.sql.catalog.iceberg.type=hadoop \
                -e "
                CALL iceberg.system.expire_snapshots(
                    table => '{{ params.table_name }}',
                    older_than => TIMESTAMP '{{ macros.ds_add(ds, -params.snapshot_retention_days) }} 00:00:00',
                    retain_last => 5
                )
                "
            """,
        )

        # Remove orphan files
        remove_orphan_files = BashOperator(
            task_id='remove_orphan_files',
            bash_command="""
            spark-sql \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                -e "
                CALL iceberg.system.remove_orphan_files(
                    table => '{{ params.table_name }}',
                    older_than => TIMESTAMP '{{ macros.ds_add(ds, -3) }} 00:00:00'
                )
                "
            """,
        )

        # Rewrite manifests (optimize metadata)
        rewrite_manifests = BashOperator(
            task_id='rewrite_manifests',
            bash_command="""
            spark-sql \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                -e "
                CALL iceberg.system.rewrite_manifests(
                    table => '{{ params.table_name }}'
                )
                "
            """,
        )

        [compact_small_files, expire_snapshots] >> remove_orphan_files >> rewrite_manifests

    # Skip maintenance placeholder
    skip_maintenance = BashOperator(
        task_id='skip_maintenance',
        bash_command='echo "No maintenance needed, table is healthy"',
    )

    # Calculate statistics
    calculate_stats = PythonOperator(
        task_id='calculate_stats',
        python_callable=calculate_maintenance_stats,
        provide_context=True,
        trigger_rule='none_failed',
    )

    # Generate maintenance report
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command="""
        echo "=== Iceberg Table Maintenance Report ==="
        echo "Table: {{ params.table_name }}"
        echo "Date: {{ ds }}"
        echo "Status: Completed"

        # Query table metadata for report
        spark-sql \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            -e "
            SELECT 'Current Snapshots: ' || COUNT(*)
            FROM iceberg.{{ params.table_name }}.snapshots;

            SELECT 'Total Files: ' || COUNT(*) || ', Total Size: ' ||
                   CAST(SUM(file_size_in_bytes) / 1024 / 1024 / 1024 AS DECIMAL(10,2)) || ' GB'
            FROM iceberg.{{ params.table_name }}.files;
            "
        """,
    )

    # Update monitoring dashboard
    update_dashboard = BashOperator(
        task_id='update_monitoring_dashboard',
        bash_command="""
        echo "Updating monitoring dashboard with maintenance metrics"
        # In production: POST metrics to Grafana, CloudWatch, etc.
        """,
    )

    # Send notification
    send_notification = BashOperator(
        task_id='send_notification',
        bash_command="""
        echo "Maintenance completed for {{ params.table_name }}"
        # In production: Send to Slack, email, PagerDuty, etc.
        """,
    )

    # Define dependencies
    analyze_table >> decide_tasks
    decide_tasks >> [maintenance_group, skip_maintenance]
    [maintenance_group, skip_maintenance] >> calculate_stats
    calculate_stats >> generate_report >> update_dashboard >> send_notification


dag.doc_md = """
# Iceberg Table Maintenance DAG

Automated maintenance for Apache Iceberg tables.

## Maintenance Operations

1. **File Compaction**
   - Combines small files into larger files
   - Target size: 512 MB (configurable)
   - Strategy: Binpack algorithm
   - Reduces metadata overhead

2. **Snapshot Expiration**
   - Removes snapshots older than 7 days
   - Retains minimum 5 snapshots
   - Reduces metadata size
   - Maintains audit trail

3. **Orphan File Removal**
   - Removes unreferenced data files
   - Files older than 3 days
   - Reclaims storage space
   - Cleans up failed writes

4. **Manifest Rewrite**
   - Optimizes manifest files
   - Improves query planning
   - Reduces metadata reads

## Schedule

Runs weekly on Sunday at 3 AM UTC

## Decision Logic

Maintenance tasks run conditionally based on analysis:
- Compaction: When >30% files are small (<10 MB)
- Snapshot expiry: When >20 snapshots or oldest >14 days
- Orphan cleanup: When orphan files detected

## Monitoring

- Pre and post-maintenance statistics
- Storage savings metrics
- Duration tracking
- Automated reporting

## Safety

- Retains minimum 5 snapshots
- 3-day grace period for orphan files
- Does not affect active readers
- Atomic operations
"""
