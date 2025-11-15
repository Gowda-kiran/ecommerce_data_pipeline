"""
Trino Client for Querying Apache Iceberg Tables
Python interface for executing Trino queries against Iceberg
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime
import logging


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrinoIcebergClient:
    """
    Client for executing Trino queries against Iceberg tables.

    Requires: pip install trino
    """

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 8080,
        user: str = 'trino',
        catalog: str = 'iceberg',
        schema: str = 'lakehouse'
    ):
        """Initialize Trino client."""
        try:
            from trino.dbapi import connect

            self.conn = connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
            )

            self.catalog = catalog
            self.schema = schema

            logger.info(f"Connected to Trino at {host}:{port}")
            logger.info(f"Using catalog: {catalog}, schema: {schema}")

        except ImportError:
            logger.error("trino package not installed. Run: pip install trino")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            raise

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.

        Args:
            query: SQL query to execute

        Returns:
            DataFrame with query results
        """
        logger.info(f"Executing query: {query[:100]}...")

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Fetch all results
            rows = cursor.fetchall()

            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)

            logger.info(f"Query returned {len(df)} rows")

            return df

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def get_table_snapshots(self, table: str) -> pd.DataFrame:
        """Get all snapshots for an Iceberg table."""
        query = f"""
        SELECT
            snapshot_id,
            parent_id,
            committed_at,
            operation,
            summary
        FROM {self.catalog}.{self.schema}."{table}$snapshots"
        ORDER BY committed_at DESC
        """
        return self.execute_query(query)

    def get_table_files(self, table: str, limit: int = 100) -> pd.DataFrame:
        """Get file information for an Iceberg table."""
        query = f"""
        SELECT
            file_path,
            file_format,
            record_count,
            file_size_in_bytes / 1024 / 1024 as file_size_mb,
            partition
        FROM {self.catalog}.{self.schema}."{table}$files"
        ORDER BY file_size_in_bytes DESC
        LIMIT {limit}
        """
        return self.execute_query(query)

    def get_partition_stats(self, table: str) -> pd.DataFrame:
        """Get partition statistics for an Iceberg table."""
        query = f"""
        SELECT
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as total_records,
            SUM(file_size_in_bytes) / 1024 / 1024 / 1024 as total_size_gb,
            AVG(file_size_in_bytes) / 1024 / 1024 as avg_file_size_mb
        FROM {self.catalog}.{self.schema}."{table}$files"
        GROUP BY partition
        ORDER BY total_size_gb DESC
        """
        return self.execute_query(query)

    def time_travel_query(
        self,
        table: str,
        timestamp: Optional[datetime] = None,
        snapshot_id: Optional[int] = None,
        query: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Execute a time travel query on Iceberg table.

        Args:
            table: Table name
            timestamp: Query as of this timestamp
            snapshot_id: Query as of this snapshot
            query: Custom SELECT clause (default: SELECT *)

        Returns:
            DataFrame with historical data
        """
        if timestamp is None and snapshot_id is None:
            raise ValueError("Must specify either timestamp or snapshot_id")

        select_clause = query if query else "SELECT *"

        if timestamp:
            time_clause = f"FOR TIMESTAMP AS OF TIMESTAMP '{timestamp}'"
        else:
            time_clause = f"FOR VERSION AS OF {snapshot_id}"

        full_query = f"""
        {select_clause}
        FROM {self.catalog}.{self.schema}.{table} {time_clause}
        LIMIT 1000
        """

        return self.execute_query(full_query)

    def get_table_health_metrics(self, table: str) -> Dict[str, Any]:
        """
        Get comprehensive health metrics for an Iceberg table.

        Returns:
            Dictionary with health metrics
        """
        logger.info(f"Analyzing health metrics for {table}")

        metrics = {}

        # Get file statistics
        files_query = f"""
        SELECT
            COUNT(*) as total_files,
            SUM(record_count) as total_records,
            SUM(file_size_in_bytes) / 1024 / 1024 / 1024 as total_size_gb,
            AVG(file_size_in_bytes) / 1024 / 1024 as avg_file_size_mb,
            MIN(file_size_in_bytes) / 1024 / 1024 as min_file_size_mb,
            MAX(file_size_in_bytes) / 1024 / 1024 as max_file_size_mb,
            SUM(CASE WHEN file_size_in_bytes < 10485760 THEN 1 ELSE 0 END) as small_files
        FROM {self.catalog}.{self.schema}."{table}$files"
        """

        files_df = self.execute_query(files_query)
        metrics['files'] = files_df.iloc[0].to_dict()

        # Get snapshot count
        snapshots_query = f"""
        SELECT COUNT(*) as snapshot_count
        FROM {self.catalog}.{self.schema}."{table}$snapshots"
        """

        snapshots_df = self.execute_query(snapshots_query)
        metrics['snapshots'] = snapshots_df.iloc[0].to_dict()

        # Get partition count
        partitions_query = f"""
        SELECT COUNT(DISTINCT partition) as partition_count
        FROM {self.catalog}.{self.schema}."{table}$files"
        """

        partitions_df = self.execute_query(partitions_query)
        metrics['partitions'] = partitions_df.iloc[0].to_dict()

        # Determine health status
        small_file_ratio = metrics['files']['small_files'] / metrics['files']['total_files']

        metrics['health_status'] = {
            'needs_compaction': small_file_ratio > 0.3,
            'needs_snapshot_expiry': metrics['snapshots']['snapshot_count'] > 20,
            'small_file_ratio': round(small_file_ratio, 3),
            'avg_file_size_mb': round(metrics['files']['avg_file_size_mb'], 2),
        }

        logger.info(f"Health metrics: {metrics['health_status']}")

        return metrics

    def run_data_quality_checks(self, table: str) -> Dict[str, Any]:
        """
        Run data quality checks on an Iceberg table.

        Returns:
            Dictionary with quality check results
        """
        logger.info(f"Running data quality checks for {table}")

        results = {}

        # Row count
        count_query = f"SELECT COUNT(*) as row_count FROM {self.catalog}.{self.schema}.{table}"
        count_df = self.execute_query(count_query)
        results['row_count'] = int(count_df.iloc[0]['row_count'])

        # Check for duplicates (if table has primary key)
        # This is table-specific, example for order_id
        try:
            duplicate_query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT order_id) as distinct_orders,
                COUNT(*) - COUNT(DISTINCT order_id) as duplicates
            FROM {self.catalog}.{self.schema}.{table}
            """
            dup_df = self.execute_query(duplicate_query)
            results['duplicates'] = dup_df.iloc[0].to_dict()
        except Exception as e:
            logger.warning(f"Duplicate check failed: {e}")
            results['duplicates'] = None

        # Data freshness
        try:
            freshness_query = f"""
            SELECT
                MAX(created_at) as latest_timestamp,
                DATE_DIFF('hour', MAX(created_at), CURRENT_TIMESTAMP) as hours_old
            FROM {self.catalog}.{self.schema}.{table}
            """
            fresh_df = self.execute_query(freshness_query)
            results['freshness'] = fresh_df.iloc[0].to_dict()
        except Exception as e:
            logger.warning(f"Freshness check failed: {e}")
            results['freshness'] = None

        logger.info(f"Quality checks complete: {results}")

        return results

    def close(self):
        """Close Trino connection."""
        if self.conn:
            self.conn.close()
            logger.info("Trino connection closed")


def example_usage():
    """Example usage of TrinoIcebergClient."""

    # Initialize client
    client = TrinoIcebergClient(
        host='localhost',
        port=8080,
        catalog='iceberg',
        schema='lakehouse'
    )

    try:
        # Example 1: Get table snapshots
        print("\n" + "="*70)
        print("EXAMPLE 1: Get Table Snapshots")
        print("="*70)

        snapshots = client.get_table_snapshots('sales')
        print(snapshots.head())

        # Example 2: Get file statistics
        print("\n" + "="*70)
        print("EXAMPLE 2: Get File Statistics")
        print("="*70)

        files = client.get_table_files('sales', limit=10)
        print(files)

        # Example 3: Get partition statistics
        print("\n" + "="*70)
        print("EXAMPLE 3: Get Partition Statistics")
        print("="*70)

        partitions = client.get_partition_stats('sales')
        print(partitions)

        # Example 4: Time travel query
        print("\n" + "="*70)
        print("EXAMPLE 4: Time Travel Query")
        print("="*70)

        historical_data = client.time_travel_query(
            table='sales',
            timestamp=datetime(2024, 1, 15, 10, 0, 0),
            query="SELECT order_id, customer_id, amount"
        )
        print(historical_data.head())

        # Example 5: Health metrics
        print("\n" + "="*70)
        print("EXAMPLE 5: Table Health Metrics")
        print("="*70)

        health = client.get_table_health_metrics('sales')
        print(f"Total Files: {health['files']['total_files']}")
        print(f"Total Size: {health['files']['total_size_gb']:.2f} GB")
        print(f"Avg File Size: {health['files']['avg_file_size_mb']:.2f} MB")
        print(f"Small Files: {health['files']['small_files']}")
        print(f"Health Status: {health['health_status']}")

        # Example 6: Data quality checks
        print("\n" + "="*70)
        print("EXAMPLE 6: Data Quality Checks")
        print("="*70)

        quality = client.run_data_quality_checks('sales')
        print(f"Row Count: {quality['row_count']:,}")
        if quality['duplicates']:
            print(f"Duplicates: {quality['duplicates']}")
        if quality['freshness']:
            print(f"Freshness: {quality['freshness']}")

        # Example 7: Custom analytical query
        print("\n" + "="*70)
        print("EXAMPLE 7: Custom Analytical Query")
        print("="*70)

        analytics_query = """
        SELECT
            DATE_TRUNC('day', order_date) as day,
            COUNT(DISTINCT order_id) as orders,
            SUM(amount) as revenue,
            COUNT(DISTINCT customer_id) as customers
        FROM iceberg.lakehouse.sales
        WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY
        GROUP BY DATE_TRUNC('day', order_date)
        ORDER BY day DESC
        """

        analytics = client.execute_query(analytics_query)
        print(analytics)

    finally:
        client.close()


if __name__ == "__main__":
    print("Trino Iceberg Client Example")
    print("="*70)

    # Run examples
    example_usage()

    print("\n✅ All examples completed successfully!")
