"""
Dimensional modeling module for creating Gold layer analytics tables.
Implements star schema with fact and dimension tables for analytics.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, monotonically_increasing_id, row_number,
    sum as _sum, count, avg, max as _max, min as _min,
    dense_rank, rank, lag, lead, first, last, when, lit
)
from pyspark.sql.window import Window
import os
from typing import Dict
import structlog

logger = structlog.get_logger()


class DimensionalModelBuilder:
    """Build dimensional model (star schema) for analytics."""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize dimensional model builder.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.silver_path = config['paths']['silver_layer']
        self.gold_path = config['paths']['gold_layer']

    def read_silver_table(self, table_name: str) -> DataFrame:
        """
        Read table from Silver layer.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame from Silver layer
        """
        path = os.path.join(self.silver_path, table_name)
        logger.info(f"Reading {table_name} from Silver layer")
        return self.spark.read.parquet(path)

    def create_dim_customer(self, customers_df: DataFrame) -> DataFrame:
        """
        Create customer dimension table.

        Args:
            customers_df: Customers DataFrame from Silver layer

        Returns:
            Customer dimension DataFrame
        """
        logger.info("Creating customer dimension")

        # Generate surrogate key
        window = Window.orderBy("customer_id")

        dim_customer = customers_df.select(
            row_number().over(window).alias("customer_key"),
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("full_name"),
            col("email"),
            col("phone_cleaned").alias("phone"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("country"),
            col("registration_date"),
            col("customer_segment"),
            col("loyalty_points"),
            col("customer_tenure_days"),
            col("is_new_customer"),
            current_timestamp().alias("effective_date"),
            lit(None).cast("timestamp").alias("expiration_date"),
            lit(True).alias("is_current")
        )

        logger.info(f"Created customer dimension with {dim_customer.count()} records")
        return dim_customer

    def create_dim_product(self, products_df: DataFrame) -> DataFrame:
        """
        Create product dimension table.

        Args:
            products_df: Products DataFrame from Silver layer

        Returns:
            Product dimension DataFrame
        """
        logger.info("Creating product dimension")

        window = Window.orderBy("product_id")

        dim_product = products_df.select(
            row_number().over(window).alias("product_key"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("subcategory"),
            col("brand"),
            col("price"),
            col("cost"),
            col("profit_margin"),
            col("stock_quantity"),
            col("is_in_stock"),
            col("stock_status"),
            col("supplier_id"),
            col("weight_kg"),
            col("is_active"),
            current_timestamp().alias("effective_date"),
            lit(None).cast("timestamp").alias("expiration_date"),
            lit(True).alias("is_current")
        )

        logger.info(f"Created product dimension with {dim_product.count()} records")
        return dim_product

    def create_dim_date(self, transactions_df: DataFrame) -> DataFrame:
        """
        Create date dimension table.

        Args:
            transactions_df: Transactions DataFrame from Silver layer

        Returns:
            Date dimension DataFrame
        """
        logger.info("Creating date dimension")

        from pyspark.sql.functions import to_date, dayofmonth, month, year, quarter, dayofweek, weekofyear

        # Extract unique dates from transactions
        dates_df = transactions_df.select(
            to_date(col("transaction_date")).alias("date")
        ).distinct()

        dim_date = dates_df.select(
            col("date").alias("date_key"),
            col("date"),
            year(col("date")).alias("year"),
            quarter(col("date")).alias("quarter"),
            month(col("date")).alias("month"),
            weekofyear(col("date")).alias("week_of_year"),
            dayofmonth(col("date")).alias("day_of_month"),
            dayofweek(col("date")).alias("day_of_week"),
            when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
            when(month(col("date")).isin([12, 1, 2]), "Winter")
            .when(month(col("date")).isin([3, 4, 5]), "Spring")
            .when(month(col("date")).isin([6, 7, 8]), "Summer")
            .otherwise("Fall").alias("season")
        )

        logger.info(f"Created date dimension with {dim_date.count()} records")
        return dim_date

    def create_fact_sales(self, transactions_df: DataFrame,
                         dim_customer: DataFrame,
                         dim_product: DataFrame,
                         dim_date: DataFrame) -> DataFrame:
        """
        Create sales fact table.

        Args:
            transactions_df: Transactions DataFrame from Silver layer
            dim_customer: Customer dimension
            dim_product: Product dimension
            dim_date: Date dimension

        Returns:
            Sales fact DataFrame
        """
        logger.info("Creating sales fact table")

        from pyspark.sql.functions import to_date

        # Join with dimensions to get surrogate keys
        fact_sales = transactions_df.alias("t") \
            .join(
                dim_customer.select("customer_key", "customer_id").alias("c"),
                col("t.customer_id") == col("c.customer_id"),
                "inner"
            ).join(
                dim_product.select("product_key", "product_id").alias("p"),
                col("t.product_id") == col("p.product_id"),
                "inner"
            ).join(
                dim_date.select("date_key", "date").alias("d"),
                to_date(col("t.transaction_date")) == col("d.date"),
                "inner"
            )

        # Select fact table columns
        fact_sales = fact_sales.select(
            monotonically_increasing_id().alias("sales_key"),
            col("t.transaction_id"),
            col("t.order_id"),
            col("c.customer_key"),
            col("p.product_key"),
            col("d.date_key"),
            col("t.transaction_date"),
            col("t.quantity"),
            col("t.unit_price"),
            col("t.discount_amount"),
            col("t.total_amount"),
            col("t.shipping_cost"),
            col("t.tax_amount"),
            col("t.net_amount"),
            col("t.gross_amount"),
            col("t.discount_percentage"),
            col("t.payment_method"),
            col("t.order_status"),
            col("t.device_type"),
            col("t.is_valid_transaction"),
            col("t.transaction_hour"),
            col("t.is_weekend")
        )

        logger.info(f"Created sales fact table with {fact_sales.count()} records")
        return fact_sales

    def create_fact_sales_aggregated(self, fact_sales: DataFrame) -> DataFrame:
        """
        Create aggregated sales fact table (daily summary).

        Args:
            fact_sales: Sales fact DataFrame

        Returns:
            Aggregated sales fact DataFrame
        """
        logger.info("Creating aggregated sales fact table")

        fact_sales_agg = fact_sales.groupBy(
            "date_key",
            "customer_key",
            "product_key"
        ).agg(
            count("sales_key").alias("transaction_count"),
            _sum("quantity").alias("total_quantity"),
            _sum("total_amount").alias("total_sales_amount"),
            _sum("discount_amount").alias("total_discount"),
            _sum("tax_amount").alias("total_tax"),
            _sum("shipping_cost").alias("total_shipping"),
            _sum("net_amount").alias("total_net_amount"),
            avg("unit_price").alias("avg_unit_price"),
            _max("total_amount").alias("max_transaction_amount"),
            _min("total_amount").alias("min_transaction_amount")
        )

        logger.info(f"Created aggregated fact table with {fact_sales_agg.count()} records")
        return fact_sales_agg

    def write_to_gold(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Write dimensional table to Gold layer.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            mode: Write mode
        """
        output_path = os.path.join(self.gold_path, table_name)
        logger.info(f"Writing {table_name} to Gold layer")

        df.write \
            .mode(mode) \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(output_path)

        logger.info(f"Successfully wrote {table_name} to Gold layer")

    def build_dimensional_model(self):
        """
        Build complete dimensional model.
        Creates all dimension and fact tables.
        """
        logger.info("Building dimensional model")

        # Read Silver layer tables
        customers_df = self.read_silver_table("customers")
        products_df = self.read_silver_table("products")
        transactions_df = self.read_silver_table("transactions")

        # Create dimension tables
        dim_customer = self.create_dim_customer(customers_df)
        dim_product = self.create_dim_product(products_df)
        dim_date = self.create_dim_date(transactions_df)

        # Create fact tables
        fact_sales = self.create_fact_sales(
            transactions_df, dim_customer, dim_product, dim_date
        )
        fact_sales_agg = self.create_fact_sales_aggregated(fact_sales)

        # Write to Gold layer
        self.write_to_gold(dim_customer, "dim_customer")
        self.write_to_gold(dim_product, "dim_product")
        self.write_to_gold(dim_date, "dim_date")
        self.write_to_gold(fact_sales, "fact_sales")
        self.write_to_gold(fact_sales_agg, "fact_sales_aggregated")

        logger.info("Dimensional model build completed")

        return {
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_date": dim_date,
            "fact_sales": fact_sales,
            "fact_sales_aggregated": fact_sales_agg
        }


def run_dimensional_modeling(spark: SparkSession, config: Dict) -> Dict[str, DataFrame]:
    """
    Run dimensional modeling process.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        Dictionary of dimensional tables
    """
    builder = DimensionalModelBuilder(spark, config)
    return builder.build_dimensional_model()


if __name__ == "__main__":
    from src.utils.spark_utils import SparkSessionManager, load_config

    spark = SparkSessionManager.get_spark_session()
    config = load_config()

    run_dimensional_modeling(spark, config)

    SparkSessionManager.stop_spark_session()
