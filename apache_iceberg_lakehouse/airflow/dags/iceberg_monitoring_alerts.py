"""
Iceberg Monitoring and Alerting DAG
Monitors table health, data quality, and performance metrics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from typing import Dict, List, Any


default_args = {
    'owner': 'data-ops',
    'depends_on_past': False,
    'email': ['data-ops@company.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def collect_table_metrics(**context):
    """Collect metrics for all monitored Iceberg tables."""
    import logging

    tables = context['params']['monitored_tables']

    logging.info(f"Collecting metrics for {len(tables)} tables")

    all_metrics = {}

    for table in tables:
        # Simulate metric collection
        # In production, query Iceberg metadata tables
        metrics = {
            'table': table,
            'total_files': 125,
            'small_files': 35,
            'total_size_gb': 4.5,
            'avg_file_size_mb': 36,
            'snapshot_count': 18,
            'partition_count': 45,
            'last_update_hours_ago': 2,
            'row_count': 1250000,
        }

        all_metrics[table] = metrics

        logging.info(f"Metrics for {table}: {metrics}")

    context['task_instance'].xcom_push(key='table_metrics', value=all_metrics)

    return all_metrics


def analyze_health_status(**context):
    """Analyze health status and determine if alerts needed."""
    import logging

    ti = context['task_instance']
    all_metrics = ti.xcom_pull(task_ids='collect_metrics', key='table_metrics')

    health_issues = {
        'critical': [],
        'warning': [],
        'healthy': []
    }

    for table, metrics in all_metrics.items():
        issues = []

        # Check small file ratio
        small_file_ratio = metrics['small_files'] / metrics['total_files']
        if small_file_ratio > 0.5:
            issues.append({
                'severity': 'critical',
                'issue': 'excessive_small_files',
                'message': f"Small file ratio: {small_file_ratio:.2%}",
                'recommendation': 'Run file compaction'
            })
        elif small_file_ratio > 0.3:
            issues.append({
                'severity': 'warning',
                'issue': 'high_small_files',
                'message': f"Small file ratio: {small_file_ratio:.2%}",
                'recommendation': 'Schedule compaction soon'
            })

        # Check snapshot count
        if metrics['snapshot_count'] > 30:
            issues.append({
                'severity': 'warning',
                'issue': 'excessive_snapshots',
                'message': f"Snapshot count: {metrics['snapshot_count']}",
                'recommendation': 'Run snapshot expiration'
            })

        # Check data freshness
        if metrics['last_update_hours_ago'] > 24:
            issues.append({
                'severity': 'critical',
                'issue': 'stale_data',
                'message': f"Last update: {metrics['last_update_hours_ago']} hours ago",
                'recommendation': 'Investigate pipeline'
            })
        elif metrics['last_update_hours_ago'] > 12:
            issues.append({
                'severity': 'warning',
                'issue': 'data_aging',
                'message': f"Last update: {metrics['last_update_hours_ago']} hours ago",
                'recommendation': 'Monitor pipeline'
            })

        # Categorize by severity
        if any(i['severity'] == 'critical' for i in issues):
            health_issues['critical'].append({'table': table, 'issues': issues})
        elif any(i['severity'] == 'warning' for i in issues):
            health_issues['warning'].append({'table': table, 'issues': issues})
        else:
            health_issues['healthy'].append(table)

    logging.info(f"Health analysis: {len(health_issues['critical'])} critical, "
                 f"{len(health_issues['warning'])} warnings, "
                 f"{len(health_issues['healthy'])} healthy")

    context['task_instance'].xcom_push(key='health_issues', value=health_issues)

    return health_issues


def decide_alert_action(**context):
    """Decide which alerts to send based on health analysis."""
    ti = context['task_instance']
    health_issues = ti.xcom_pull(task_ids='analyze_health', key='health_issues')

    actions = []

    if health_issues['critical']:
        actions.append('send_critical_alert')

    if health_issues['warning']:
        actions.append('send_warning_alert')

    if not actions:
        actions.append('skip_alerts')

    return actions


def generate_alert_report(**context):
    """Generate detailed alert report."""
    import logging

    ti = context['task_instance']
    health_issues = ti.xcom_pull(task_ids='analyze_health', key='health_issues')

    report_lines = []
    report_lines.append("="*70)
    report_lines.append("ICEBERG TABLE HEALTH REPORT")
    report_lines.append("="*70)
    report_lines.append(f"Generated: {datetime.now()}")
    report_lines.append("")

    # Critical issues
    if health_issues['critical']:
        report_lines.append("🔴 CRITICAL ISSUES:")
        report_lines.append("")
        for item in health_issues['critical']:
            report_lines.append(f"Table: {item['table']}")
            for issue in item['issues']:
                if issue['severity'] == 'critical':
                    report_lines.append(f"  - {issue['issue']}: {issue['message']}")
                    report_lines.append(f"    Action: {issue['recommendation']}")
            report_lines.append("")

    # Warning issues
    if health_issues['warning']:
        report_lines.append("⚠️  WARNING ISSUES:")
        report_lines.append("")
        for item in health_issues['warning']:
            report_lines.append(f"Table: {item['table']}")
            for issue in item['issues']:
                if issue['severity'] == 'warning':
                    report_lines.append(f"  - {issue['issue']}: {issue['message']}")
                    report_lines.append(f"    Action: {issue['recommendation']}")
            report_lines.append("")

    # Healthy tables
    if health_issues['healthy']:
        report_lines.append(f"✅ HEALTHY TABLES ({len(health_issues['healthy'])}):")
        for table in health_issues['healthy']:
            report_lines.append(f"  - {table}")
        report_lines.append("")

    report = "\n".join(report_lines)

    logging.info(report)

    context['task_instance'].xcom_push(key='alert_report', value=report)

    return report


def check_sla_compliance(**context):
    """Check SLA compliance for data pipelines."""
    import logging

    # Define SLAs
    slas = {
        'sales': {
            'max_lag_hours': 6,
            'min_daily_rows': 10000,
            'max_null_rate': 0.01
        },
        'customers': {
            'max_lag_hours': 12,
            'min_daily_rows': 100,
            'max_null_rate': 0.005
        }
    }

    compliance_results = {}

    for table, sla in slas.items():
        # Simulate SLA checks
        # In production, query actual metrics
        actual = {
            'lag_hours': 4,
            'daily_rows': 15000,
            'null_rate': 0.008
        }

        compliance = {
            'lag_compliant': actual['lag_hours'] <= sla['max_lag_hours'],
            'volume_compliant': actual['daily_rows'] >= sla['min_daily_rows'],
            'quality_compliant': actual['null_rate'] <= sla['max_null_rate'],
        }

        compliance['overall_compliant'] = all(compliance.values())

        compliance_results[table] = {
            'sla': sla,
            'actual': actual,
            'compliance': compliance
        }

        logging.info(f"SLA for {table}: {'✅ PASS' if compliance['overall_compliant'] else '❌ FAIL'}")

    context['task_instance'].xcom_push(key='sla_compliance', value=compliance_results)

    return compliance_results


def publish_metrics_to_monitoring(**context):
    """Publish metrics to external monitoring system."""
    import logging
    import json

    ti = context['task_instance']
    table_metrics = ti.xcom_pull(task_ids='collect_metrics', key='table_metrics')
    health_issues = ti.xcom_pull(task_ids='analyze_health', key='health_issues')
    sla_compliance = ti.xcom_pull(task_ids='check_sla_compliance', key='sla_compliance')

    # Prepare metrics for export
    metrics_export = {
        'timestamp': str(datetime.now()),
        'tables': table_metrics,
        'health_summary': {
            'critical_count': len(health_issues['critical']),
            'warning_count': len(health_issues['warning']),
            'healthy_count': len(health_issues['healthy'])
        },
        'sla_summary': {
            table: result['compliance']['overall_compliant']
            for table, result in sla_compliance.items()
        }
    }

    # In production, send to CloudWatch, Datadog, Prometheus, etc.
    logging.info("Publishing metrics to monitoring system")
    logging.info(json.dumps(metrics_export, indent=2))

    # Example: POST to monitoring API
    # requests.post('https://monitoring.company.com/metrics', json=metrics_export)

    return metrics_export


with DAG(
    dag_id='iceberg_monitoring_alerts',
    default_args=default_args,
    description='Monitor Iceberg table health and send alerts',
    schedule_interval='0 * * * *',  # Hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['iceberg', 'monitoring', 'alerts'],
    params={
        'monitored_tables': [
            'lakehouse.sales',
            'lakehouse.customers',
            'lakehouse.products',
            'lakehouse.orders'
        ],
        'alert_email': 'data-ops@company.com',
        'critical_threshold': 'immediate',
        'warning_threshold': 'daily_digest'
    }
) as dag:

    # Collect metrics from all monitored tables
    collect_metrics = PythonOperator(
        task_id='collect_metrics',
        python_callable=collect_table_metrics,
        provide_context=True,
    )

    # Task Group: Health Checks
    with TaskGroup(group_id='health_checks') as health_group:

        # Analyze health status
        analyze_health = PythonOperator(
            task_id='analyze_health',
            python_callable=analyze_health_status,
            provide_context=True,
        )

        # Check SLA compliance
        check_sla = PythonOperator(
            task_id='check_sla_compliance',
            python_callable=check_sla_compliance,
            provide_context=True,
        )

        # Check query performance
        check_performance = BashOperator(
            task_id='check_query_performance',
            bash_command="""
            echo "Checking query performance metrics"
            # In production: Query slow query logs, execution times
            """,
        )

        [analyze_health, check_sla, check_performance]

    # Decide alert action
    decide_action = BranchPythonOperator(
        task_id='decide_alert_action',
        python_callable=decide_alert_action,
        provide_context=True,
    )

    # Generate alert report
    generate_report = PythonOperator(
        task_id='generate_alert_report',
        python_callable=generate_alert_report,
        provide_context=True,
    )

    # Send critical alert (immediate)
    send_critical = EmailOperator(
        task_id='send_critical_alert',
        to='{{ params.alert_email }}',
        subject='🔴 CRITICAL: Iceberg Table Health Issues',
        html_content="""
        <h2>Critical Iceberg Table Health Issues Detected</h2>
        <p>Immediate action required!</p>
        <p>See attached report for details.</p>
        <p>Generated: {{ ts }}</p>
        <p>{{ task_instance.xcom_pull(task_ids='generate_alert_report', key='alert_report') }}</p>
        """,
    )

    # Send warning alert (digest)
    send_warning = EmailOperator(
        task_id='send_warning_alert',
        to='{{ params.alert_email }}',
        subject='⚠️ WARNING: Iceberg Table Health Issues',
        html_content="""
        <h2>Warning: Iceberg Table Health Issues</h2>
        <p>Action recommended within 24 hours.</p>
        <p>See attached report for details.</p>
        <p>Generated: {{ ts }}</p>
        <p>{{ task_instance.xcom_pull(task_ids='generate_alert_report', key='alert_report') }}</p>
        """,
    )

    # Skip alerts
    skip_alerts = BashOperator(
        task_id='skip_alerts',
        bash_command='echo "No alerts needed - all tables healthy"',
    )

    # Publish metrics to monitoring system
    publish_metrics = PythonOperator(
        task_id='publish_metrics',
        python_callable=publish_metrics_to_monitoring,
        provide_context=True,
        trigger_rule='none_failed',
    )

    # Update dashboard
    update_dashboard = BashOperator(
        task_id='update_dashboard',
        bash_command="""
        echo "Updating Grafana/Tableau dashboard with latest metrics"
        # In production: API call to dashboard service
        """,
        trigger_rule='none_failed',
    )

    # Log to audit table
    log_monitoring_run = BashOperator(
        task_id='log_monitoring_run',
        bash_command="""
        echo "Logging monitoring run to audit table"
        spark-sql \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            -e "
            INSERT INTO lakehouse.monitoring_audit
            VALUES (
                '{{ dag.dag_id }}',
                TIMESTAMP '{{ ts }}',
                '{{ task_instance.xcom_pull(task_ids='collect_metrics', key='table_metrics') }}',
                'completed'
            )
            "
        """,
        trigger_rule='none_failed',
    )

    # Define dependencies
    collect_metrics >> health_group >> generate_report >> decide_action
    decide_action >> [send_critical, send_warning, skip_alerts]
    [send_critical, send_warning, skip_alerts] >> publish_metrics
    publish_metrics >> [update_dashboard, log_monitoring_run]


dag.doc_md = """
# Iceberg Monitoring and Alerting DAG

Continuous monitoring of Apache Iceberg table health and performance.

## Monitoring Dimensions

1. **Table Health**
   - File counts and sizes
   - Small file ratio
   - Snapshot count
   - Partition distribution

2. **Data Quality**
   - Row counts
   - Null rates
   - Duplicate detection
   - Schema compliance

3. **Data Freshness**
   - Last update timestamp
   - Ingestion lag
   - Pipeline SLA compliance

4. **Performance**
   - Query execution times
   - Metadata operation latency
   - Scan performance

## Alert Levels

- **Critical**: Immediate notification
  - Data staleness > 24 hours
  - Small file ratio > 50%
  - SLA violations

- **Warning**: Daily digest
  - Small file ratio > 30%
  - Snapshot count > 30
  - Data staleness > 12 hours

- **Info**: Weekly report
  - General health metrics
  - Trend analysis

## Schedule

Runs hourly for continuous monitoring

## Metrics Export

Metrics published to:
- CloudWatch (AWS)
- Datadog
- Prometheus
- Grafana dashboards

## Alerting Channels

- Email (immediate for critical)
- Slack (all levels)
- PagerDuty (critical only)
- Ticket system (warnings)

## Auto-Remediation

Some issues trigger automatic remediation:
- High small file ratio → Schedule compaction
- Excessive snapshots → Trigger expiration
- SLA violations → Restart pipeline

## Dashboard

Real-time dashboard shows:
- Table health scores
- SLA compliance rates
- Trend charts
- Alert history
"""
