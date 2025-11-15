"""
Apache Iceberg Data Quality Framework
Comprehensive data quality checks, validation, and monitoring
"""

import sys
sys.path.append('../..')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    stddev, countDistinct, when, isnan, isnull, lit, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime
from config.spark_iceberg_config import create_iceberg_spark_session


class DataQualityFramework:
    """
    Comprehensive data quality framework for Iceberg tables
    """

    def __init__(self, spark):
        self.spark = spark
        self.results = []

    def check_completeness(self, table_name, required_columns):
        """Check for null values in required columns."""

        print(f"\n{'='*70}")
        print(f"COMPLETENESS CHECK: {table_name}")
        print(f"{'='*70}")

        df = self.spark.table(table_name)
        total_rows = df.count()

        print(f"\n📊 Total Rows: {total_rows}")
        print(f"📋 Required Columns: {', '.join(required_columns)}")

        completeness_results = []

        for column in required_columns:
            if column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

                status = "✅ PASS" if null_count == 0 else "❌ FAIL"

                result = {
                    'table': table_name,
                    'check': 'completeness',
                    'column': column,
                    'total_rows': total_rows,
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2),
                    'status': 'PASS' if null_count == 0 else 'FAIL'
                }

                completeness_results.append(result)
                self.results.append(result)

                print(f"\n   {status} {column}")
                print(f"      Null count: {null_count} ({null_percentage:.2f}%)")
            else:
                print(f"\n   ⚠️  Column '{column}' not found in table")

        return completeness_results

    def check_uniqueness(self, table_name, unique_columns):
        """Check for duplicate values in unique columns."""

        print(f"\n{'='*70}")
        print(f"UNIQUENESS CHECK: {table_name}")
        print(f"{'='*70}")

        df = self.spark.table(table_name)
        total_rows = df.count()

        print(f"\n📊 Total Rows: {total_rows}")
        print(f"📋 Unique Columns: {', '.join(unique_columns)}")

        uniqueness_results = []

        for column in unique_columns:
            if column in df.columns:
                distinct_count = df.select(column).distinct().count()
                duplicate_count = total_rows - distinct_count
                duplicate_percentage = (duplicate_count / total_rows * 100) if total_rows > 0 else 0

                status = "✅ PASS" if duplicate_count == 0 else "❌ FAIL"

                result = {
                    'table': table_name,
                    'check': 'uniqueness',
                    'column': column,
                    'total_rows': total_rows,
                    'distinct_count': distinct_count,
                    'duplicate_count': duplicate_count,
                    'duplicate_percentage': round(duplicate_percentage, 2),
                    'status': 'PASS' if duplicate_count == 0 else 'FAIL'
                }

                uniqueness_results.append(result)
                self.results.append(result)

                print(f"\n   {status} {column}")
                print(f"      Distinct values: {distinct_count}")
                print(f"      Duplicates: {duplicate_count} ({duplicate_percentage:.2f}%)")
            else:
                print(f"\n   ⚠️  Column '{column}' not found in table")

        return uniqueness_results

    def check_validity(self, table_name, column, valid_values):
        """Check if column values are within a valid set."""

        print(f"\n{'='*70}")
        print(f"VALIDITY CHECK: {table_name}.{column}")
        print(f"{'='*70}")

        df = self.spark.table(table_name)
        total_rows = df.count()

        print(f"\n📊 Total Rows: {total_rows}")
        print(f"📋 Valid Values: {', '.join(str(v) for v in valid_values)}")

        invalid_count = df.filter(~col(column).isin(valid_values)).count()
        invalid_percentage = (invalid_count / total_rows * 100) if total_rows > 0 else 0

        status = "✅ PASS" if invalid_count == 0 else "❌ FAIL"

        result = {
            'table': table_name,
            'check': 'validity',
            'column': column,
            'total_rows': total_rows,
            'invalid_count': invalid_count,
            'invalid_percentage': round(invalid_percentage, 2),
            'valid_values': valid_values,
            'status': 'PASS' if invalid_count == 0 else 'FAIL'
        }

        self.results.append(result)

        print(f"\n   {status} {column}")
        print(f"      Invalid values: {invalid_count} ({invalid_percentage:.2f}%)")

        if invalid_count > 0:
            print("\n   Invalid value distribution:")
            df.filter(~col(column).isin(valid_values)) \
                .groupBy(column).count() \
                .orderBy(col("count").desc()) \
                .show(10)

        return result

    def check_range(self, table_name, column, min_value=None, max_value=None):
        """Check if numeric values are within a valid range."""

        print(f"\n{'='*70}")
        print(f"RANGE CHECK: {table_name}.{column}")
        print(f"{'='*70}")

        df = self.spark.table(table_name)
        total_rows = df.count()

        print(f"\n📊 Total Rows: {total_rows}")

        # Get actual min/max
        stats = df.agg(
            _min(column).alias("actual_min"),
            _max(column).alias("actual_max"),
            avg(column).alias("average")
        ).collect()[0]

        actual_min = stats.actual_min
        actual_max = stats.actual_max
        average = stats.average

        print(f"📋 Actual Range: [{actual_min}, {actual_max}]")
        print(f"📋 Average: {average:.2f}")

        if min_value is not None:
            print(f"📋 Expected Min: {min_value}")
        if max_value is not None:
            print(f"📋 Expected Max: {max_value}")

        # Check violations
        out_of_range_count = 0
        if min_value is not None and max_value is not None:
            out_of_range_count = df.filter(
                (col(column) < min_value) | (col(column) > max_value)
            ).count()
        elif min_value is not None:
            out_of_range_count = df.filter(col(column) < min_value).count()
        elif max_value is not None:
            out_of_range_count = df.filter(col(column) > max_value).count()

        out_of_range_percentage = (out_of_range_count / total_rows * 100) if total_rows > 0 else 0

        status = "✅ PASS" if out_of_range_count == 0 else "❌ FAIL"

        result = {
            'table': table_name,
            'check': 'range',
            'column': column,
            'total_rows': total_rows,
            'actual_min': actual_min,
            'actual_max': actual_max,
            'expected_min': min_value,
            'expected_max': max_value,
            'out_of_range_count': out_of_range_count,
            'out_of_range_percentage': round(out_of_range_percentage, 2),
            'status': 'PASS' if out_of_range_count == 0 else 'FAIL'
        }

        self.results.append(result)

        print(f"\n   {status} {column}")
        print(f"      Out of range: {out_of_range_count} ({out_of_range_percentage:.2f}%)")

        return result

    def check_format(self, table_name, column, regex_pattern, pattern_description):
        """Check if string values match a specific format."""

        print(f"\n{'='*70}")
        print(f"FORMAT CHECK: {table_name}.{column}")
        print(f"{'='*70}")

        df = self.spark.table(table_name)
        total_rows = df.count()

        print(f"\n📊 Total Rows: {total_rows}")
        print(f"📋 Pattern: {pattern_description}")
        print(f"📋 Regex: {regex_pattern}")

        # Check format violations
        invalid_format_count = df.filter(
            ~col(column).rlike(regex_pattern) & col(column).isNotNull()
        ).count()

        invalid_format_percentage = (invalid_format_count / total_rows * 100) if total_rows > 0 else 0

        status = "✅ PASS" if invalid_format_count == 0 else "❌ FAIL"

        result = {
            'table': table_name,
            'check': 'format',
            'column': column,
            'total_rows': total_rows,
            'regex_pattern': regex_pattern,
            'invalid_format_count': invalid_format_count,
            'invalid_format_percentage': round(invalid_format_percentage, 2),
            'status': 'PASS' if invalid_format_count == 0 else 'FAIL'
        }

        self.results.append(result)

        print(f"\n   {status} {column}")
        print(f"      Invalid format: {invalid_format_count} ({invalid_format_percentage:.2f}%)")

        if invalid_format_count > 0 and invalid_format_count <= 10:
            print("\n   Sample invalid values:")
            df.filter(~col(column).rlike(regex_pattern) & col(column).isNotNull()) \
                .select(column).limit(10).show(truncate=False)

        return result

    def check_freshness(self, table_name, timestamp_column, max_age_hours=24):
        """Check data freshness based on timestamp column."""

        print(f"\n{'='*70}")
        print(f"FRESHNESS CHECK: {table_name}.{timestamp_column}")
        print(f"{'='*70}")

        df = self.spark.table(table_name)
        total_rows = df.count()

        print(f"\n📊 Total Rows: {total_rows}")
        print(f"📋 Max Age: {max_age_hours} hours")

        # Get latest timestamp
        latest_timestamp = df.agg(_max(timestamp_column).alias("latest")).collect()[0].latest

        if latest_timestamp:
            current_time = datetime.now()
            age_hours = (current_time - latest_timestamp).total_seconds() / 3600

            status = "✅ PASS" if age_hours <= max_age_hours else "❌ FAIL"

            result = {
                'table': table_name,
                'check': 'freshness',
                'column': timestamp_column,
                'latest_timestamp': latest_timestamp,
                'age_hours': round(age_hours, 2),
                'max_age_hours': max_age_hours,
                'status': 'PASS' if age_hours <= max_age_hours else 'FAIL'
            }

            self.results.append(result)

            print(f"\n   {status} {timestamp_column}")
            print(f"      Latest timestamp: {latest_timestamp}")
            print(f"      Age: {age_hours:.2f} hours")

            return result
        else:
            print("\n   ⚠️  No timestamp values found")
            return None

    def check_referential_integrity(self, parent_table, parent_key, child_table, foreign_key):
        """Check referential integrity between tables."""

        print(f"\n{'='*70}")
        print(f"REFERENTIAL INTEGRITY CHECK")
        print(f"{'='*70}")

        print(f"\n📋 Parent: {parent_table}.{parent_key}")
        print(f"📋 Child: {child_table}.{foreign_key}")

        parent_df = self.spark.table(parent_table)
        child_df = self.spark.table(child_table)

        # Find orphaned records (foreign keys with no matching parent key)
        orphaned = child_df.join(
            parent_df,
            child_df[foreign_key] == parent_df[parent_key],
            "left_anti"
        )

        orphaned_count = orphaned.count()
        total_child_rows = child_df.count()
        orphaned_percentage = (orphaned_count / total_child_rows * 100) if total_child_rows > 0 else 0

        status = "✅ PASS" if orphaned_count == 0 else "❌ FAIL"

        result = {
            'check': 'referential_integrity',
            'parent_table': parent_table,
            'parent_key': parent_key,
            'child_table': child_table,
            'foreign_key': foreign_key,
            'total_child_rows': total_child_rows,
            'orphaned_count': orphaned_count,
            'orphaned_percentage': round(orphaned_percentage, 2),
            'status': 'PASS' if orphaned_count == 0 else 'FAIL'
        }

        self.results.append(result)

        print(f"\n   {status} Referential Integrity")
        print(f"      Total child records: {total_child_rows}")
        print(f"      Orphaned records: {orphaned_count} ({orphaned_percentage:.2f}%)")

        if orphaned_count > 0 and orphaned_count <= 10:
            print("\n   Sample orphaned records:")
            orphaned.limit(10).show(truncate=False)

        return result

    def generate_summary_report(self):
        """Generate summary report of all data quality checks."""

        print(f"\n{'='*70}")
        print("DATA QUALITY SUMMARY REPORT")
        print(f"{'='*70}")

        if not self.results:
            print("\n   No checks have been run yet")
            return

        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r['status'] == 'PASS')
        failed_checks = total_checks - passed_checks
        pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0

        print(f"\n📊 Overall Statistics:")
        print(f"   Total Checks: {total_checks}")
        print(f"   Passed: {passed_checks} ({'✅ ' if passed_checks > 0 else ''})")
        print(f"   Failed: {failed_checks} ({'❌ ' if failed_checks > 0 else ''})")
        print(f"   Pass Rate: {pass_rate:.1f}%")

        # Group by check type
        check_types = {}
        for result in self.results:
            check_type = result['check']
            if check_type not in check_types:
                check_types[check_type] = {'total': 0, 'passed': 0}
            check_types[check_type]['total'] += 1
            if result['status'] == 'PASS':
                check_types[check_type]['passed'] += 1

        print(f"\n📋 By Check Type:")
        for check_type, stats in check_types.items():
            pass_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"   {check_type.upper()}: {stats['passed']}/{stats['total']} ({pass_rate:.1f}%)")

        # Show failed checks
        failed = [r for r in self.results if r['status'] == 'FAIL']
        if failed:
            print(f"\n❌ Failed Checks ({len(failed)}):")
            for result in failed:
                table = result.get('table', result.get('child_table', 'N/A'))
                check = result['check']
                column = result.get('column', result.get('foreign_key', 'N/A'))
                print(f"   • {table}.{column} - {check}")

        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'pass_rate': pass_rate,
            'results': self.results
        }


def example_complete_quality_check(spark):
    """Run comprehensive quality checks on sample data."""

    print("\n" + "="*70)
    print("COMPLETE DATA QUALITY EXAMPLE")
    print("="*70)

    # Create sample table
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")
    spark.sql("DROP TABLE IF EXISTS lakehouse.quality_test_products")

    spark.sql("""
        CREATE TABLE lakehouse.quality_test_products (
            product_id STRING,
            product_name STRING,
            category STRING,
            price DOUBLE,
            quantity INT,
            sku STRING,
            status STRING,
            created_at TIMESTAMP
        )
        USING iceberg
    """)

    # Insert sample data with some quality issues
    spark.sql("""
        INSERT INTO lakehouse.quality_test_products VALUES
        ('P001', 'Laptop', 'Electronics', 999.99, 10, 'SKU-001', 'active', CURRENT_TIMESTAMP),
        ('P002', 'Mouse', 'Electronics', 29.99, 5, 'SKU-002', 'active', CURRENT_TIMESTAMP),
        ('P003', 'Desk', 'Furniture', 299.99, 3, 'SKU-003', 'active', CURRENT_TIMESTAMP),
        ('P004', NULL, 'Electronics', 199.99, 0, 'SKU-004', 'active', CURRENT_TIMESTAMP),  -- Missing name
        ('P005', 'Monitor', 'Electronics', -50.00, 2, 'BAD', 'inactive', CURRENT_TIMESTAMP),  -- Negative price, bad SKU
        ('P005', 'Monitor Duplicate', 'Electronics', 399.99, 2, 'SKU-005', 'active', CURRENT_TIMESTAMP),  -- Duplicate ID
        ('P006', 'Chair', 'Furniture', 1500.00, 1, 'SKU-006', 'unknown', CURRENT_TIMESTAMP),  -- Invalid status
        ('P007', 'Keyboard', NULL, 79.99, 8, 'SKU-007', 'active', CURRENT_TIMESTAMP)  -- Missing category
    """)

    # Initialize quality framework
    dq = DataQualityFramework(spark)

    # Run checks
    dq.check_completeness('lakehouse.quality_test_products', ['product_id', 'product_name', 'category'])
    dq.check_uniqueness('lakehouse.quality_test_products', ['product_id'])
    dq.check_validity('lakehouse.quality_test_products', 'status', ['active', 'inactive', 'discontinued'])
    dq.check_range('lakehouse.quality_test_products', 'price', min_value=0.01, max_value=10000.00)
    dq.check_format('lakehouse.quality_test_products', 'sku', r'^SKU-\d{3}$', 'SKU-XXX format')
    dq.check_freshness('lakehouse.quality_test_products', 'created_at', max_age_hours=24)

    # Generate report
    summary = dq.generate_summary_report()

    return summary


def main():
    """Main execution function."""

    # Create Spark session
    spark = create_iceberg_spark_session(
        app_name="Iceberg Data Quality Framework"
    )

    try:
        # Run complete quality check example
        summary = example_complete_quality_check(spark)

        print("\n" + "="*70)
        print("✅ DATA QUALITY FRAMEWORK DEMONSTRATION COMPLETE!")
        print("="*70)

        print("\n📚 FRAMEWORK CAPABILITIES:")
        print("   ✅ Completeness checks (null detection)")
        print("   ✅ Uniqueness checks (duplicate detection)")
        print("   ✅ Validity checks (value domain validation)")
        print("   ✅ Range checks (numeric bounds)")
        print("   ✅ Format checks (regex pattern matching)")
        print("   ✅ Freshness checks (data recency)")
        print("   ✅ Referential integrity (foreign key validation)")
        print("   ✅ Comprehensive reporting")

        print("\n💡 INTEGRATION WITH ICEBERG:")
        print("   • Leverage snapshot isolation for consistent checks")
        print("   • Use time travel for trend analysis")
        print("   • Partition-level quality metrics")
        print("   • Track quality metrics over snapshots")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
