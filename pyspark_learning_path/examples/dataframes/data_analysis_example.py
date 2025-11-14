"""
Practical Data Analysis with PySpark DataFrames
Demonstrates common data analysis operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    when, lit, round as spark_round, concat, datediff, current_date
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_sample_data(spark):
    """Create sample sales data for analysis."""

    # Define schema
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("region", StringType(), False)
    ])

    # Sample data
    data = [
        ("ORD001", "CUST001", "Laptop", "Electronics", 2, 1200.00, "North"),
        ("ORD002", "CUST002", "Mouse", "Electronics", 5, 25.00, "South"),
        ("ORD003", "CUST001", "Keyboard", "Electronics", 3, 75.00, "North"),
        ("ORD004", "CUST003", "Desk", "Furniture", 1, 450.00, "East"),
        ("ORD005", "CUST004", "Chair", "Furniture", 4, 200.00, "West"),
        ("ORD006", "CUST002", "Monitor", "Electronics", 2, 350.00, "South"),
        ("ORD007", "CUST005", "Desk", "Furniture", 1, 450.00, "North"),
        ("ORD008", "CUST003", "Laptop", "Electronics", 1, 1200.00, "East"),
        ("ORD009", "CUST006", "Mouse", "Electronics", 10, 25.00, "West"),
        ("ORD010", "CUST004", "Keyboard", "Electronics", 2, 75.00, "West"),
    ]

    return spark.createDataFrame(data, schema)

def main():
    """Main execution function with various DataFrame operations."""

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Data Analysis Example") \
        .master("local[*]") \
        .getOrCreate()

    # Create sample data
    df = create_sample_data(spark)

    print("=" * 80)
    print("PYSPARK DATA ANALYSIS EXAMPLES")
    print("=" * 80)

    # 1. Basic DataFrame Operations
    print("\n1. BASIC DATAFRAME INFO")
    print("-" * 40)
    print(f"Total rows: {df.count()}")
    print(f"Columns: {df.columns}")
    print("\nSchema:")
    df.printSchema()
    print("\nFirst 5 rows:")
    df.show(5)

    # 2. Add Calculated Columns
    print("\n2. ADDING CALCULATED COLUMNS")
    print("-" * 40)
    df_with_totals = df.withColumn("total_amount",
                                   spark_round(col("quantity") * col("unit_price"), 2))
    df_with_totals.select("order_id", "product", "quantity", "unit_price", "total_amount").show()

    # 3. Filtering
    print("\n3. FILTERING DATA")
    print("-" * 40)
    print("High-value orders (total > $500):")
    high_value = df_with_totals.filter(col("total_amount") > 500)
    high_value.select("order_id", "product", "total_amount").show()

    # 4. Group By and Aggregations
    print("\n4. AGGREGATIONS BY CATEGORY")
    print("-" * 40)
    category_stats = df_with_totals.groupBy("category").agg(
        count("order_id").alias("num_orders"),
        _sum("quantity").alias("total_quantity"),
        _sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value")
    ).orderBy(col("total_revenue").desc())
    category_stats.show()

    # 5. Multiple Aggregations
    print("\n5. REGIONAL SALES ANALYSIS")
    print("-" * 40)
    regional_stats = df_with_totals.groupBy("region").agg(
        count("*").alias("orders"),
        _sum("total_amount").alias("revenue"),
        _max("total_amount").alias("max_order"),
        _min("total_amount").alias("min_order"),
        spark_round(avg("total_amount"), 2).alias("avg_order")
    ).orderBy(col("revenue").desc())
    regional_stats.show()

    # 6. Conditional Columns
    print("\n6. CATEGORIZING ORDERS")
    print("-" * 40)
    df_categorized = df_with_totals.withColumn(
        "order_size",
        when(col("total_amount") < 100, "Small")
        .when(col("total_amount") < 500, "Medium")
        .otherwise("Large")
    )
    df_categorized.select("order_id", "total_amount", "order_size").show()

    # 7. Customer Analysis
    print("\n7. CUSTOMER PURCHASE BEHAVIOR")
    print("-" * 40)
    customer_stats = df_with_totals.groupBy("customer_id").agg(
        count("order_id").alias("num_orders"),
        _sum("total_amount").alias("total_spent"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value")
    ).orderBy(col("total_spent").desc())

    # Add customer segment
    customer_segments = customer_stats.withColumn(
        "segment",
        when(col("total_spent") > 1000, "Premium")
        .when(col("total_spent") > 500, "Regular")
        .otherwise("Occasional")
    )
    customer_segments.show()

    # 8. Product Analysis
    print("\n8. PRODUCT POPULARITY")
    print("-" * 40)
    product_stats = df_with_totals.groupBy("product").agg(
        count("*").alias("times_ordered"),
        _sum("quantity").alias("units_sold"),
        _sum("total_amount").alias("revenue")
    ).orderBy(col("revenue").desc())
    product_stats.show()

    # 9. Using SQL
    print("\n9. SQL QUERY EXAMPLE")
    print("-" * 40)
    df_with_totals.createOrReplaceTempView("sales")

    sql_result = spark.sql("""
        SELECT
            category,
            COUNT(*) as num_orders,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order
        FROM sales
        WHERE total_amount > 100
        GROUP BY category
        ORDER BY revenue DESC
    """)
    sql_result.show()

    # 10. Window Functions Preview
    print("\n10. TOP PRODUCTS PER CATEGORY")
    print("-" * 40)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window_spec = Window.partitionBy("category").orderBy(col("total_amount").desc())

    top_products = df_with_totals.withColumn(
        "rank_in_category",
        row_number().over(window_spec)
    ).filter(col("rank_in_category") <= 2)

    top_products.select("category", "product", "total_amount", "rank_in_category").show()

    # Summary Statistics
    print("\n11. SUMMARY STATISTICS")
    print("-" * 40)
    df_with_totals.select("total_amount").describe().show()

    # Stop Spark Session
    spark.stop()

    print("\n" + "=" * 80)
    print("✅ Data analysis examples completed!")
    print("=" * 80)

if __name__ == "__main__":
    main()
