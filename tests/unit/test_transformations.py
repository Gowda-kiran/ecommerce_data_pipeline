"""
Unit tests for data transformation functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.transformation.data_transformer import DataTransformer


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("TestEcommercePipeline") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'paths': {
            'bronze_layer': 'data/bronze',
            'silver_layer': 'data/silver'
        },
        'business_rules': {
            'min_order_amount': 0.01,
            'max_order_amount': 1000000,
            'valid_payment_methods': ['credit_card', 'debit_card', 'paypal', 'wallet'],
            'valid_order_statuses': ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'refunded']
        }
    }


@pytest.fixture
def sample_customers_data(spark):
    """Create sample customer data for testing."""
    data = [
        ('CUST001', 'John', 'Doe', 'JOHN.DOE@EXAMPLE.COM', '555-1234', 'NY', '10001'),
        ('CUST002', ' Jane ', ' Smith ', 'jane.smith@example.com', '555-5678', 'CA', '90001'),
        ('CUST003', 'Bob', 'Johnson', None, '555-9012', 'TX', '73301'),  # Missing email
    ]

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


class TestDataTransformer:
    """Test cases for DataTransformer class."""

    def test_customer_email_normalization(self, spark, sample_config, sample_customers_data):
        """Test that customer emails are properly normalized to lowercase."""
        transformer = DataTransformer(spark, sample_config)

        # Add required fields for transformation
        from pyspark.sql.functions import lit, current_timestamp
        df = sample_customers_data.withColumn("address", lit("123 Main St")) \
            .withColumn("city", lit("New York")) \
            .withColumn("country", lit("USA")) \
            .withColumn("registration_date", lit("2023-01-01")) \
            .withColumn("customer_segment", lit("Regular")) \
            .withColumn("loyalty_points", lit(100)) \
            .withColumn("ingestion_timestamp", current_timestamp())

        result = transformer.transform_customers(df)

        # Collect emails
        emails = [row.email for row in result.select("email").collect()]

        # All emails should be lowercase
        assert all(email.islower() for email in emails if email is not None)

    def test_customer_name_trimming(self, spark, sample_config, sample_customers_data):
        """Test that customer names are properly trimmed."""
        transformer = DataTransformer(spark, sample_config)

        from pyspark.sql.functions import lit, current_timestamp
        df = sample_customers_data.withColumn("address", lit("123 Main St")) \
            .withColumn("city", lit("New York")) \
            .withColumn("country", lit("USA")) \
            .withColumn("registration_date", lit("2023-01-01")) \
            .withColumn("customer_segment", lit("Regular")) \
            .withColumn("loyalty_points", lit(100)) \
            .withColumn("ingestion_timestamp", current_timestamp())

        result = transformer.transform_customers(df)

        # Get Jane's record (had spaces)
        jane = result.filter(result.customer_id == 'CUST002').first()

        assert jane.first_name == 'Jane'
        assert jane.last_name == 'Smith'

    def test_customer_deduplication(self, spark, sample_config):
        """Test that duplicate customers are removed."""
        transformer = DataTransformer(spark, sample_config)

        from pyspark.sql.functions import lit, current_timestamp

        # Create data with duplicates
        data = [
            ('CUST001', 'John', 'Doe', 'john@example.com'),
            ('CUST001', 'John', 'Doe', 'john@example.com'),  # Duplicate
            ('CUST002', 'Jane', 'Smith', 'jane@example.com'),
        ]

        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema) \
            .withColumn("address", lit("123 Main St")) \
            .withColumn("city", lit("New York")) \
            .withColumn("state", lit("NY")) \
            .withColumn("zip_code", lit("10001")) \
            .withColumn("country", lit("USA")) \
            .withColumn("phone", lit("555-1234")) \
            .withColumn("registration_date", lit("2023-01-01")) \
            .withColumn("customer_segment", lit("Regular")) \
            .withColumn("loyalty_points", lit(100)) \
            .withColumn("ingestion_timestamp", current_timestamp())

        result = transformer.transform_customers(df)

        # Should only have 2 unique customers
        assert result.count() == 2

    def test_transaction_validation_flag(self, spark, sample_config):
        """Test that invalid transactions are properly flagged."""
        transformer = DataTransformer(spark, sample_config)

        from pyspark.sql.functions import lit, current_timestamp

        # Create transactions with valid and invalid data
        data = [
            ('TXN001', 'ORD001', 'CUST001', 'PROD001', 100.00, 'credit_card', 'delivered'),  # Valid
            ('TXN002', 'ORD002', 'CUST002', 'PROD002', -10.00, 'credit_card', 'delivered'),  # Negative amount
            ('TXN003', 'ORD003', 'CUST003', 'PROD003', 50.00, 'invalid_method', 'delivered'),  # Invalid payment method
        ]

        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("payment_method", StringType(), True),
            StructField("order_status", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema) \
            .withColumn("transaction_date", lit("2023-01-01 10:00:00")) \
            .withColumn("quantity", lit(1)) \
            .withColumn("unit_price", lit(100.00)) \
            .withColumn("discount_amount", lit(0.00)) \
            .withColumn("shipping_cost", lit(5.00)) \
            .withColumn("tax_amount", lit(10.00)) \
            .withColumn("shipping_address", lit("123 Main St")) \
            .withColumn("device_type", lit("mobile")) \
            .withColumn("session_id", lit("SESSION001")) \
            .withColumn("ingestion_timestamp", current_timestamp())

        result = transformer.transform_transactions(df)

        # Check validation flags
        txn1 = result.filter(result.transaction_id == 'TXN001').first()
        txn2 = result.filter(result.transaction_id == 'TXN002').first()

        assert txn1.is_valid_transaction == True  # Valid transaction
        assert txn2.is_valid_transaction == False  # Negative amount


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
