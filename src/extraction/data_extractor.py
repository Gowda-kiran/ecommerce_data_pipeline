"""
Data extraction module for reading raw data into Bronze layer.
Implements the Extract phase of the ETL pipeline.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
import os
from typing import Dict, List
import structlog

logger = structlog.get_logger()


class DataExtractor:
    """Extract data from raw sources into Bronze layer."""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the data extractor.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.raw_path = config['paths']['raw_data']
        self.bronze_path = config['paths']['bronze_layer']

    def extract_csv(self, source_name: str, schema=None) -> DataFrame:
        """
        Extract data from CSV file.

        Args:
            source_name: Name of the source (e.g., 'transactions', 'customers')
            schema: Optional schema for the DataFrame

        Returns:
            DataFrame with raw data
        """
        filename = self.config['sources'].get(source_name)
        if not filename:
            raise ValueError(f"Source {source_name} not found in configuration")

        file_path = os.path.join(self.raw_path, filename)

        logger.info(f"Extracting data from {file_path}")

        # Read CSV with options
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true" if schema is None else "false") \
            .option("dateFormat", "yyyy-MM-dd") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .csv(file_path)

        if schema:
            df = self.spark.createDataFrame(df.rdd, schema)

        # Add metadata columns for lineage tracking
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", lit(filename))

        logger.info(f"Extracted {df.count()} records from {source_name}")

        return df

    def extract_all_sources(self) -> Dict[str, DataFrame]:
        """
        Extract all configured data sources.

        Returns:
            Dictionary mapping source names to DataFrames
        """
        dataframes = {}

        for source_name in self.config['sources'].keys():
            try:
                df = self.extract_csv(source_name)
                dataframes[source_name] = df
                logger.info(f"Successfully extracted {source_name}")
            except Exception as e:
                logger.error(f"Failed to extract {source_name}: {str(e)}")
                raise

        return dataframes

    def load_to_bronze(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Load extracted data to Bronze layer.

        Args:
            df: DataFrame to load
            table_name: Name of the table
            mode: Write mode (overwrite, append, etc.)
        """
        output_path = os.path.join(self.bronze_path, table_name)

        logger.info(f"Loading {table_name} to Bronze layer at {output_path}")

        # Write to parquet for better performance and compression
        df.write \
            .mode(mode) \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(output_path)

        logger.info(f"Successfully loaded {df.count()} records to {table_name}")

    def extract_and_load_all(self):
        """
        Extract all sources and load them to Bronze layer.
        This is the main entry point for the extraction process.
        """
        logger.info("Starting data extraction process")

        dataframes = self.extract_all_sources()

        for source_name, df in dataframes.items():
            self.load_to_bronze(df, source_name)

        logger.info("Data extraction process completed")

        return dataframes


def run_extraction(spark: SparkSession, config: Dict) -> Dict[str, DataFrame]:
    """
    Run the data extraction process.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        Dictionary of extracted DataFrames
    """
    extractor = DataExtractor(spark, config)
    return extractor.extract_and_load_all()


if __name__ == "__main__":
    from src.utils.spark_utils import SparkSessionManager, load_config

    # Get Spark session and configuration
    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    # Run extraction
    run_extraction(spark, config)

    # Stop Spark session
    SparkSessionManager.stop_spark_session()
