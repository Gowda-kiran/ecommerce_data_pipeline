"""
Data quality validation framework for ensuring data integrity.
Implements comprehensive data quality checks including completeness,
accuracy, consistency, and validity.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, isnan, sum as _sum, avg, stddev
from typing import Dict, List, Tuple
import structlog
from dataclasses import dataclass
from datetime import datetime

logger = structlog.get_logger()


@dataclass
class QualityCheckResult:
    """Result of a data quality check."""
    check_name: str
    table_name: str
    passed: bool
    metric_value: float
    threshold: float
    timestamp: str
    message: str


class DataQualityChecker:
    """Comprehensive data quality validation."""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize data quality checker.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.quality_config = config.get('quality', {})
        self.results: List[QualityCheckResult] = []

    def check_completeness(self, df: DataFrame, table_name: str,
                          columns: List[str] = None) -> List[QualityCheckResult]:
        """
        Check data completeness (null value percentage).

        Args:
            df: DataFrame to check
            table_name: Name of the table
            columns: List of columns to check (all if None)

        Returns:
            List of QualityCheckResult
        """
        if columns is None:
            columns = df.columns

        total_rows = df.count()
        min_completeness = self.quality_config.get('min_completeness', 0.95)
        results = []

        for column in columns:
            # Skip metadata columns
            if column in ['ingestion_timestamp', 'source_file']:
                continue

            null_count = df.filter(col(column).isNull()).count()
            completeness = 1 - (null_count / total_rows) if total_rows > 0 else 0

            passed = completeness >= min_completeness

            result = QualityCheckResult(
                check_name=f"completeness_{column}",
                table_name=table_name,
                passed=passed,
                metric_value=completeness,
                threshold=min_completeness,
                timestamp=datetime.now().isoformat(),
                message=f"Column '{column}' completeness: {completeness:.2%} "
                       f"(threshold: {min_completeness:.2%})"
            )
            results.append(result)

            if not passed:
                logger.warning(f"Completeness check failed for {table_name}.{column}: "
                             f"{completeness:.2%} < {min_completeness:.2%}")

        return results

    def check_duplicates(self, df: DataFrame, table_name: str,
                        key_columns: List[str]) -> QualityCheckResult:
        """
        Check for duplicate records.

        Args:
            df: DataFrame to check
            table_name: Name of the table
            key_columns: Columns that define uniqueness

        Returns:
            QualityCheckResult
        """
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        duplicate_rate = 1 - (distinct_rows / total_rows) if total_rows > 0 else 0

        max_duplicate_rate = self.quality_config.get('max_duplicate_rate', 0.01)
        passed = duplicate_rate <= max_duplicate_rate

        result = QualityCheckResult(
            check_name="duplicate_check",
            table_name=table_name,
            passed=passed,
            metric_value=duplicate_rate,
            threshold=max_duplicate_rate,
            timestamp=datetime.now().isoformat(),
            message=f"Duplicate rate: {duplicate_rate:.2%} (threshold: {max_duplicate_rate:.2%})"
        )

        if not passed:
            logger.warning(f"Duplicate check failed for {table_name}: "
                         f"{duplicate_rate:.2%} > {max_duplicate_rate:.2%}")

        return result

    def check_value_range(self, df: DataFrame, table_name: str,
                         column: str, min_value=None, max_value=None) -> QualityCheckResult:
        """
        Check if values are within expected range.

        Args:
            df: DataFrame to check
            table_name: Name of the table
            column: Column to check
            min_value: Minimum expected value
            max_value: Maximum expected value

        Returns:
            QualityCheckResult
        """
        total_rows = df.count()
        out_of_range_count = 0

        if min_value is not None:
            out_of_range_count += df.filter(col(column) < min_value).count()

        if max_value is not None:
            out_of_range_count += df.filter(col(column) > max_value).count()

        validity_rate = 1 - (out_of_range_count / total_rows) if total_rows > 0 else 0
        passed = validity_rate >= 0.99  # 99% of values should be in range

        result = QualityCheckResult(
            check_name=f"range_check_{column}",
            table_name=table_name,
            passed=passed,
            metric_value=validity_rate,
            threshold=0.99,
            timestamp=datetime.now().isoformat(),
            message=f"Column '{column}' range validity: {validity_rate:.2%}"
        )

        if not passed:
            logger.warning(f"Range check failed for {table_name}.{column}")

        return result

    def check_referential_integrity(self, df1: DataFrame, df2: DataFrame,
                                   join_column: str, table1_name: str,
                                   table2_name: str) -> QualityCheckResult:
        """
        Check referential integrity between two tables.

        Args:
            df1: First DataFrame
            df2: Second DataFrame (reference table)
            join_column: Column to join on
            table1_name: Name of first table
            table2_name: Name of second (reference) table

        Returns:
            QualityCheckResult
        """
        # Count records in df1 that don't have a match in df2
        left_anti = df1.join(df2, on=join_column, how="left_anti")
        orphan_count = left_anti.count()
        total_count = df1.count()

        integrity_rate = 1 - (orphan_count / total_count) if total_count > 0 else 0
        passed = integrity_rate >= 0.99

        result = QualityCheckResult(
            check_name=f"referential_integrity_{join_column}",
            table_name=f"{table1_name}_to_{table2_name}",
            passed=passed,
            metric_value=integrity_rate,
            threshold=0.99,
            timestamp=datetime.now().isoformat(),
            message=f"Referential integrity on '{join_column}': {integrity_rate:.2%}"
        )

        if not passed:
            logger.warning(f"Referential integrity check failed: {orphan_count} "
                         f"orphan records found in {table1_name}")

        return result

    def check_statistical_anomalies(self, df: DataFrame, table_name: str,
                                   column: str) -> QualityCheckResult:
        """
        Check for statistical anomalies using standard deviation.

        Args:
            df: DataFrame to check
            table_name: Name of the table
            column: Numeric column to check

        Returns:
            QualityCheckResult
        """
        stats = df.select(avg(col(column)).alias("mean"),
                         stddev(col(column)).alias("stddev")).first()

        mean_val = stats['mean']
        stddev_val = stats['stddev']

        if mean_val is None or stddev_val is None:
            return QualityCheckResult(
                check_name=f"anomaly_check_{column}",
                table_name=table_name,
                passed=True,
                metric_value=0.0,
                threshold=0.05,
                timestamp=datetime.now().isoformat(),
                message=f"Skipped anomaly check for {column} (insufficient data)"
            )

        # Count outliers (beyond 3 standard deviations)
        outlier_count = df.filter(
            (col(column) < mean_val - 3 * stddev_val) |
            (col(column) > mean_val + 3 * stddev_val)
        ).count()

        total_count = df.count()
        anomaly_rate = outlier_count / total_count if total_count > 0 else 0

        max_anomaly_score = self.quality_config.get('max_anomaly_score', 0.05)
        passed = anomaly_rate <= max_anomaly_score

        result = QualityCheckResult(
            check_name=f"anomaly_check_{column}",
            table_name=table_name,
            passed=passed,
            metric_value=anomaly_rate,
            threshold=max_anomaly_score,
            timestamp=datetime.now().isoformat(),
            message=f"Anomaly rate for '{column}': {anomaly_rate:.2%}"
        )

        if not passed:
            logger.warning(f"Anomaly check failed for {table_name}.{column}")

        return result

    def run_all_checks(self, dataframes: Dict[str, DataFrame]) -> List[QualityCheckResult]:
        """
        Run all quality checks on provided DataFrames.

        Args:
            dataframes: Dictionary of table name to DataFrame

        Returns:
            List of all quality check results
        """
        logger.info("Running comprehensive data quality checks")
        all_results = []

        # Customers checks
        if 'customers' in dataframes:
            df = dataframes['customers']
            all_results.extend(self.check_completeness(df, 'customers',
                             ['customer_id', 'email', 'first_name', 'last_name']))
            all_results.append(self.check_duplicates(df, 'customers', ['customer_id']))

        # Products checks
        if 'products' in dataframes:
            df = dataframes['products']
            all_results.extend(self.check_completeness(df, 'products',
                             ['product_id', 'product_name', 'price']))
            all_results.append(self.check_duplicates(df, 'products', ['product_id']))
            all_results.append(self.check_value_range(df, 'products', 'price',
                                                      min_value=0, max_value=100000))

        # Transactions checks
        if 'transactions' in dataframes:
            df = dataframes['transactions']
            all_results.extend(self.check_completeness(df, 'transactions',
                             ['transaction_id', 'order_id', 'customer_id', 'product_id']))
            all_results.append(self.check_duplicates(df, 'transactions', ['transaction_id']))
            all_results.append(self.check_value_range(df, 'transactions', 'total_amount',
                                                      min_value=0, max_value=1000000))
            all_results.append(self.check_statistical_anomalies(df, 'transactions', 'total_amount'))

            # Referential integrity checks
            if 'customers' in dataframes:
                all_results.append(self.check_referential_integrity(
                    df, dataframes['customers'], 'customer_id',
                    'transactions', 'customers'
                ))

            if 'products' in dataframes:
                all_results.append(self.check_referential_integrity(
                    df, dataframes['products'], 'product_id',
                    'transactions', 'products'
                ))

        self.results = all_results

        # Summary
        total_checks = len(all_results)
        passed_checks = sum(1 for r in all_results if r.passed)
        failed_checks = total_checks - passed_checks

        logger.info(f"Quality checks completed: {passed_checks}/{total_checks} passed, "
                   f"{failed_checks} failed")

        return all_results

    def generate_report(self) -> Dict:
        """
        Generate a quality check report.

        Returns:
            Dictionary containing quality report
        """
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        failed_checks = total_checks - passed_checks

        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_checks': total_checks,
                'passed': passed_checks,
                'failed': failed_checks,
                'pass_rate': passed_checks / total_checks if total_checks > 0 else 0
            },
            'failed_checks': [
                {
                    'check': r.check_name,
                    'table': r.table_name,
                    'metric': r.metric_value,
                    'threshold': r.threshold,
                    'message': r.message
                }
                for r in self.results if not r.passed
            ],
            'all_results': [
                {
                    'check': r.check_name,
                    'table': r.table_name,
                    'passed': r.passed,
                    'metric': r.metric_value,
                    'threshold': r.threshold
                }
                for r in self.results
            ]
        }

        return report


def run_quality_checks(spark: SparkSession, config: Dict,
                       dataframes: Dict[str, DataFrame]) -> List[QualityCheckResult]:
    """
    Run data quality checks.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        dataframes: Dictionary of DataFrames to check

    Returns:
        List of quality check results
    """
    checker = DataQualityChecker(spark, config)
    results = checker.run_all_checks(dataframes)

    # Generate and log report
    report = checker.generate_report()
    logger.info(f"Quality Report: {report['summary']}")

    return results
