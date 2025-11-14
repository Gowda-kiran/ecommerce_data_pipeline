"""
Airflow DAG for E-commerce Data Pipeline.
Orchestrates the complete ETL process: Extract -> Transform -> Load -> Quality Check
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import yaml
import os
import sys

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/..'))

from src.utils.spark_utils import SparkSessionManager, load_config
from src.extraction.data_extractor import run_extraction
from src.transformation.data_transformer import run_transformation
from src.quality.data_quality_checks import run_quality_checks
from src.loading.dimensional_model import run_dimensional_modeling


# Load configuration
config = load_config('config/config.yaml')
airflow_config = config.get('airflow', {})

# Default arguments for the DAG
default_args = {
    'owner': airflow_config.get('default_args', {}).get('owner', 'data_engineering'),
    'depends_on_past': airflow_config.get('default_args', {}).get('depends_on_past', False),
    'email': ['data-team@example.com'],
    'email_on_failure': airflow_config.get('default_args', {}).get('email_on_failure', True),
    'email_on_retry': airflow_config.get('default_args', {}).get('email_on_retry', False),
    'retries': airflow_config.get('default_args', {}).get('retries', 3),
    'retry_delay': timedelta(minutes=airflow_config.get('default_args', {}).get('retry_delay_minutes', 5)),
    'execution_timeout': timedelta(hours=2),
}


def extract_data(**context):
    """
    Extract data from raw sources to Bronze layer.
    """
    print("Starting data extraction...")
    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    dataframes = run_extraction(spark, config)

    print(f"Extraction completed. Extracted {len(dataframes)} tables.")
    return {"status": "success", "tables_extracted": list(dataframes.keys())}


def transform_data(**context):
    """
    Transform data from Bronze to Silver layer.
    """
    print("Starting data transformation...")
    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    dataframes = run_transformation(spark, config)

    print(f"Transformation completed. Transformed {len(dataframes)} tables.")
    return {"status": "success", "tables_transformed": list(dataframes.keys())}


def run_quality_validation(**context):
    """
    Run data quality checks on Silver layer data.
    """
    print("Starting data quality validation...")
    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    # Read Silver layer data
    silver_path = config['paths']['silver_layer']
    dataframes = {
        'customers': spark.read.parquet(os.path.join(silver_path, 'customers')),
        'products': spark.read.parquet(os.path.join(silver_path, 'products')),
        'transactions': spark.read.parquet(os.path.join(silver_path, 'transactions'))
    }

    results = run_quality_checks(spark, config, dataframes)

    # Check if quality checks passed
    failed_checks = [r for r in results if not r.passed]
    if failed_checks:
        print(f"WARNING: {len(failed_checks)} quality checks failed!")
        for check in failed_checks:
            print(f"  - {check.table_name}.{check.check_name}: {check.message}")

    total_checks = len(results)
    passed_checks = total_checks - len(failed_checks)

    print(f"Quality validation completed. {passed_checks}/{total_checks} checks passed.")

    return {
        "status": "success",
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": len(failed_checks)
    }


def build_dimensional_model(**context):
    """
    Build dimensional model in Gold layer.
    """
    print("Starting dimensional model build...")
    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    dataframes = run_dimensional_modeling(spark, config)

    print(f"Dimensional model build completed. Created {len(dataframes)} tables.")
    return {"status": "success", "tables_created": list(dataframes.keys())}


def cleanup(**context):
    """
    Cleanup and stop Spark session.
    """
    print("Cleaning up resources...")
    SparkSessionManager.stop_spark_session()
    print("Cleanup completed.")
    return {"status": "success"}


# Create the DAG
with DAG(
    dag_id=airflow_config.get('dag_id', 'ecommerce_etl_pipeline'),
    default_args=default_args,
    description='E-commerce data pipeline with Extract, Transform, Load, and Quality checks',
    schedule_interval=airflow_config.get('schedule_interval', '0 2 * * *'),  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'etl', 'data-warehouse'],
    max_active_runs=1,
) as dag:

    # Task 1: Extract data from raw sources to Bronze layer
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    # Task 2: Transform data from Bronze to Silver layer
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Task 3: Run data quality checks
    quality_check_task = PythonOperator(
        task_id='quality_validation',
        python_callable=run_quality_validation,
        provide_context=True,
    )

    # Task 4: Build dimensional model in Gold layer
    dimensional_model_task = PythonOperator(
        task_id='build_dimensional_model',
        python_callable=build_dimensional_model,
        provide_context=True,
    )

    # Task 5: Cleanup resources
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        provide_context=True,
        trigger_rule='all_done',  # Run even if upstream tasks fail
    )

    # Define task dependencies
    # Extract -> Transform -> Quality Check -> Dimensional Model -> Cleanup
    extract_task >> transform_task >> quality_check_task >> dimensional_model_task >> cleanup_task


# Additional utility functions for the DAG
def get_dag_stats():
    """Get statistics about DAG runs."""
    return {
        'dag_id': dag.dag_id,
        'schedule_interval': dag.schedule_interval,
        'tasks': [task.task_id for task in dag.tasks]
    }


if __name__ == "__main__":
    # Test DAG structure
    print("DAG Structure:")
    print(f"DAG ID: {dag.dag_id}")
    print(f"Schedule: {dag.schedule_interval}")
    print(f"Tasks: {[task.task_id for task in dag.tasks]}")
    print("\nTask Dependencies:")
    for task in dag.tasks:
        print(f"  {task.task_id}: upstream={[t.task_id for t in task.upstream_list]}, "
              f"downstream={[t.task_id for t in task.downstream_list]}")
