"""
Spark utility functions for creating and managing Spark sessions.
"""

from pyspark.sql import SparkSession
import yaml
import os
from typing import Dict, Any


class SparkSessionManager:
    """Manage Spark session creation and configuration."""

    _instance = None

    @staticmethod
    def get_spark_session(config_path: str = "config/config.yaml") -> SparkSession:
        """
        Get or create a Spark session with configuration from YAML file.

        Args:
            config_path: Path to configuration file

        Returns:
            SparkSession instance
        """
        if SparkSessionManager._instance is not None:
            return SparkSessionManager._instance

        # Load configuration
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        spark_config = config.get('spark', {})
        app_name = spark_config.get('app_name', 'EcommerceDataPipeline')
        master = spark_config.get('master', 'local[*]')
        configs = spark_config.get('configs', {})

        # Create Spark session builder
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master)

        # Apply configurations
        for key, value in configs.items():
            builder = builder.config(key, value)

        SparkSessionManager._instance = builder.getOrCreate()

        # Set log level
        SparkSessionManager._instance.sparkContext.setLogLevel("WARN")

        return SparkSessionManager._instance

    @staticmethod
    def stop_spark_session():
        """Stop the current Spark session."""
        if SparkSessionManager._instance is not None:
            SparkSessionManager._instance.stop()
            SparkSessionManager._instance = None


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_data_path(layer: str, filename: str = None, config_path: str = "config/config.yaml") -> str:
    """
    Get the full path for a data file in a specific layer.

    Args:
        layer: Data layer (raw_data, bronze_layer, silver_layer, gold_layer)
        filename: Optional filename to append
        config_path: Path to configuration file

    Returns:
        Full path to data file or directory
    """
    config = load_config(config_path)
    base_path = config['paths'].get(layer, f'data/{layer}')

    if filename:
        return os.path.join(base_path, filename)
    return base_path
