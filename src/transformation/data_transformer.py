"""
Data transformation module for processing Bronze to Silver layer.
Implements cleansing, standardization, and enrichment.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, trim, upper, lower, regexp_replace, to_date, to_timestamp,
    coalesce, lit, sha2, concat_ws, datediff, current_date, year, month,
    dayofmonth, hour, dayofweek, sum as _sum, count, avg, round as spark_round
)
from pyspark.sql.window import Window
import os
from typing import Dict
import structlog

logger = structlog.get_logger()


class DataTransformer:
    """Transform data from Bronze to Silver layer with cleansing and enrichment."""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the data transformer.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.bronze_path = config['paths']['bronze_layer']
        self.silver_path = config['paths']['silver_layer']
        self.business_rules = config.get('business_rules', {})

    def read_bronze_table(self, table_name: str) -> DataFrame:
        """
        Read table from Bronze layer.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame from Bronze layer
        """
        path = os.path.join(self.bronze_path, table_name)
        logger.info(f"Reading {table_name} from Bronze layer")
        return self.spark.read.parquet(path)

    def transform_customers(self, df: DataFrame) -> DataFrame:
        """
        Transform and cleanse customer data.

        Args:
            df: Raw customer DataFrame

        Returns:
            Transformed customer DataFrame
        """
        logger.info("Transforming customer data")

        # Cleanse and standardize
        transformed = df.select(
            col("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            regexp_replace(col("phone"), "[^0-9]", "").alias("phone_cleaned"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            upper(trim(col("state"))).alias("state"),
            regexp_replace(col("zip_code"), "[^0-9]", "").alias("zip_code"),
            upper(trim(col("country"))).alias("country"),
            to_date(col("registration_date")).alias("registration_date"),
            col("customer_segment"),
            col("loyalty_points"),
            col("ingestion_timestamp")
        )

        # Add derived columns
        transformed = transformed.withColumn(
            "full_name",
            concat_ws(" ", col("first_name"), col("last_name"))
        ).withColumn(
            "customer_tenure_days",
            datediff(current_date(), col("registration_date"))
        ).withColumn(
            "is_new_customer",
            when(col("customer_tenure_days") < 30, True).otherwise(False)
        )

        # Remove duplicates based on customer_id
        transformed = transformed.dropDuplicates(["customer_id"])

        # Filter out invalid records
        transformed = transformed.filter(
            (col("email").isNotNull()) &
            (col("customer_id").isNotNull())
        )

        logger.info(f"Transformed {transformed.count()} customer records")
        return transformed

    def transform_products(self, df: DataFrame) -> DataFrame:
        """
        Transform and cleanse product data.

        Args:
            df: Raw product DataFrame

        Returns:
            Transformed product DataFrame
        """
        logger.info("Transforming product data")

        # Cleanse and standardize
        transformed = df.select(
            col("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("subcategory")).alias("subcategory"),
            trim(col("brand")).alias("brand"),
            col("price").cast("decimal(10,2)").alias("price"),
            col("cost").cast("decimal(10,2)").alias("cost"),
            col("stock_quantity").cast("int").alias("stock_quantity"),
            col("supplier_id"),
            col("weight_kg").cast("decimal(10,2)").alias("weight_kg"),
            col("is_active").cast("boolean").alias("is_active"),
            col("ingestion_timestamp")
        )

        # Add derived columns
        transformed = transformed.withColumn(
            "profit_margin",
            spark_round(((col("price") - col("cost")) / col("price")) * 100, 2)
        ).withColumn(
            "is_in_stock",
            when(col("stock_quantity") > 0, True).otherwise(False)
        ).withColumn(
            "stock_status",
            when(col("stock_quantity") == 0, "Out of Stock")
            .when(col("stock_quantity") < 10, "Low Stock")
            .when(col("stock_quantity") < 50, "Medium Stock")
            .otherwise("In Stock")
        )

        # Remove duplicates
        transformed = transformed.dropDuplicates(["product_id"])

        # Filter out invalid records
        transformed = transformed.filter(
            (col("product_id").isNotNull()) &
            (col("price") > 0) &
            (col("cost") >= 0)
        )

        logger.info(f"Transformed {transformed.count()} product records")
        return transformed

    def transform_transactions(self, df: DataFrame) -> DataFrame:
        """
        Transform and cleanse transaction data.

        Args:
            df: Raw transaction DataFrame

        Returns:
            Transformed transaction DataFrame
        """
        logger.info("Transforming transaction data")

        # Cleanse and standardize
        transformed = df.select(
            col("transaction_id"),
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            to_timestamp(col("transaction_date")).alias("transaction_date"),
            col("quantity").cast("int").alias("quantity"),
            col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),
            col("total_amount").cast("decimal(10,2)").alias("total_amount"),
            lower(trim(col("payment_method"))).alias("payment_method"),
            col("shipping_cost").cast("decimal(10,2)").alias("shipping_cost"),
            col("tax_amount").cast("decimal(10,2)").alias("tax_amount"),
            lower(trim(col("order_status"))).alias("order_status"),
            trim(col("shipping_address")).alias("shipping_address"),
            lower(trim(col("device_type"))).alias("device_type"),
            col("session_id"),
            col("ingestion_timestamp")
        )

        # Add derived columns
        transformed = transformed.withColumn(
            "transaction_year",
            year(col("transaction_date"))
        ).withColumn(
            "transaction_month",
            month(col("transaction_date"))
        ).withColumn(
            "transaction_day",
            dayofmonth(col("transaction_date"))
        ).withColumn(
            "transaction_hour",
            hour(col("transaction_date"))
        ).withColumn(
            "day_of_week",
            dayofweek(col("transaction_date"))
        ).withColumn(
            "is_weekend",
            when(col("day_of_week").isin([1, 7]), True).otherwise(False)
        ).withColumn(
            "gross_amount",
            col("quantity") * col("unit_price")
        ).withColumn(
            "net_amount",
            col("total_amount") + col("shipping_cost") + col("tax_amount")
        ).withColumn(
            "discount_percentage",
            spark_round((col("discount_amount") / (col("quantity") * col("unit_price"))) * 100, 2)
        )

        # Apply business rules for data quality
        min_amount = self.business_rules.get('min_order_amount', 0.01)
        max_amount = self.business_rules.get('max_order_amount', 1000000)
        valid_payment_methods = self.business_rules.get('valid_payment_methods', [])
        valid_statuses = self.business_rules.get('valid_order_statuses', [])

        # Flag invalid transactions
        transformed = transformed.withColumn(
            "is_valid_transaction",
            when(
                (col("total_amount") >= min_amount) &
                (col("total_amount") <= max_amount) &
                (col("quantity") > 0) &
                (col("payment_method").isin(valid_payment_methods) | col("payment_method").isNull()) &
                (col("order_status").isin(valid_statuses)),
                True
            ).otherwise(False)
        )

        # Remove duplicates
        transformed = transformed.dropDuplicates(["transaction_id"])

        logger.info(f"Transformed {transformed.count()} transaction records")
        return transformed

    def write_to_silver(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Write transformed data to Silver layer.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            mode: Write mode
        """
        output_path = os.path.join(self.silver_path, table_name)
        logger.info(f"Writing {table_name} to Silver layer")

        df.write \
            .mode(mode) \
            .format("parquet") \
            .option("compression", "snappy") \
            .partitionBy("transaction_year", "transaction_month") if table_name == "transactions" \
            else df.write.mode(mode).format("parquet").option("compression", "snappy") \
            .save(output_path)

        logger.info(f"Successfully wrote {table_name} to Silver layer")

    def transform_all(self):
        """
        Transform all tables from Bronze to Silver layer.
        """
        logger.info("Starting transformation process")

        # Read from Bronze
        customers_df = self.read_bronze_table("customers")
        products_df = self.read_bronze_table("products")
        transactions_df = self.read_bronze_table("transactions")

        # Transform
        customers_transformed = self.transform_customers(customers_df)
        products_transformed = self.transform_products(products_df)
        transactions_transformed = self.transform_transactions(transactions_df)

        # Write to Silver
        self.write_to_silver(customers_transformed, "customers")
        self.write_to_silver(products_transformed, "products")
        self.write_to_silver(transactions_transformed, "transactions")

        logger.info("Transformation process completed")

        return {
            "customers": customers_transformed,
            "products": products_transformed,
            "transactions": transactions_transformed
        }


def run_transformation(spark: SparkSession, config: Dict) -> Dict[str, DataFrame]:
    """
    Run the data transformation process.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        Dictionary of transformed DataFrames
    """
    transformer = DataTransformer(spark, config)
    return transformer.transform_all()


if __name__ == "__main__":
    from src.utils.spark_utils import SparkSessionManager, load_config

    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    run_transformation(spark, config)

    SparkSessionManager.stop_spark_session()
