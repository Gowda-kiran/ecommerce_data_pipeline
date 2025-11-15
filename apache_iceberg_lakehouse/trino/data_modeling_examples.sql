-- ============================================================================
-- Apache Iceberg Data Modeling with Trino
-- Comprehensive examples of querying and modeling Iceberg tables with Trino
-- ============================================================================

-- ============================================================================
-- PART 1: CATALOG AND SCHEMA SETUP
-- ============================================================================

-- Show available catalogs
SHOW CATALOGS;

-- Use Iceberg catalog
USE iceberg.lakehouse;

-- Show tables in lakehouse
SHOW TABLES;

-- Describe table structure
DESCRIBE lakehouse.sales;

-- Show table properties
SHOW CREATE TABLE lakehouse.sales;


-- ============================================================================
-- PART 2: TIME TRAVEL QUERIES WITH TRINO
-- ============================================================================

-- Query current state of table
SELECT
    order_id,
    customer_id,
    product_id,
    amount,
    order_date
FROM lakehouse.sales
WHERE order_date >= DATE '2024-01-01'
LIMIT 10;


-- Query table as of specific timestamp (Time Travel)
SELECT *
FROM lakehouse.sales FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:00:00'
WHERE order_date = DATE '2024-01-15'
LIMIT 10;


-- Query table as of specific version/snapshot
-- First, get snapshot IDs
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary
FROM lakehouse."sales$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- Query specific snapshot (replace with actual snapshot_id)
SELECT *
FROM lakehouse.sales FOR VERSION AS OF 1234567890
LIMIT 10;


-- Compare data between two time points
WITH current_data AS (
    SELECT customer_id, SUM(amount) as current_total
    FROM lakehouse.sales
    GROUP BY customer_id
),
historical_data AS (
    SELECT customer_id, SUM(amount) as historical_total
    FROM lakehouse.sales FOR TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00'
    GROUP BY customer_id
)
SELECT
    COALESCE(c.customer_id, h.customer_id) as customer_id,
    COALESCE(c.current_total, 0) as current_total,
    COALESCE(h.historical_total, 0) as historical_total,
    COALESCE(c.current_total, 0) - COALESCE(h.historical_total, 0) as growth
FROM current_data c
FULL OUTER JOIN historical_data h ON c.customer_id = h.customer_id
ORDER BY growth DESC
LIMIT 20;


-- ============================================================================
-- PART 3: METADATA QUERIES
-- ============================================================================

-- View all snapshots
SELECT
    snapshot_id,
    parent_id,
    committed_at,
    operation,
    manifest_list,
    summary
FROM lakehouse."sales$snapshots"
ORDER BY committed_at DESC;


-- View data files and their statistics
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes / 1024 / 1024 as file_size_mb,
    partition,
    column_sizes,
    value_counts,
    null_value_counts
FROM lakehouse."sales$files"
ORDER BY file_size_in_bytes DESC
LIMIT 20;


-- View partition information
SELECT
    partition,
    COUNT(*) as file_count,
    SUM(record_count) as total_records,
    SUM(file_size_in_bytes) / 1024 / 1024 / 1024 as total_size_gb,
    MIN(file_size_in_bytes) / 1024 / 1024 as min_file_size_mb,
    MAX(file_size_in_bytes) / 1024 / 1024 as max_file_size_mb,
    AVG(file_size_in_bytes) / 1024 / 1024 as avg_file_size_mb
FROM lakehouse."sales$files"
GROUP BY partition
ORDER BY total_size_gb DESC;


-- View table history
SELECT
    made_current_at,
    snapshot_id,
    parent_id,
    is_current_ancestor
FROM lakehouse."sales$history"
ORDER BY made_current_at DESC
LIMIT 10;


-- View manifest files
SELECT
    path,
    length / 1024 / 1024 as size_mb,
    partition_spec_id,
    added_snapshot_id,
    added_data_files_count,
    existing_data_files_count,
    deleted_data_files_count
FROM lakehouse."sales$manifests"
ORDER BY added_snapshot_id DESC
LIMIT 10;


-- ============================================================================
-- PART 4: DIMENSIONAL MODELING QUERIES
-- ============================================================================

-- Create dimensional model views on top of Iceberg tables

-- Dimension: Customer
CREATE OR REPLACE VIEW lakehouse.dim_customer AS
SELECT DISTINCT
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state,
    customer_country,
    customer_segment
FROM lakehouse.customers
WHERE is_current = true;


-- Dimension: Product with SCD Type 2
CREATE OR REPLACE VIEW lakehouse.dim_product_current AS
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost,
    effective_date,
    expiration_date,
    is_current,
    version
FROM lakehouse.products
WHERE is_current = true;


-- Fact: Sales with aggregations
CREATE OR REPLACE VIEW lakehouse.fact_sales_daily AS
SELECT
    order_date,
    customer_id,
    product_id,
    store_id,
    COUNT(DISTINCT order_id) as order_count,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_amount,
    SUM(discount) as total_discount,
    SUM(tax) as total_tax,
    SUM(amount - discount + tax) as net_amount,
    AVG(amount) as avg_order_amount
FROM lakehouse.sales
GROUP BY order_date, customer_id, product_id, store_id;


-- ============================================================================
-- PART 5: ANALYTICAL QUERIES
-- ============================================================================

-- Customer Lifetime Value (CLV)
WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(amount) as lifetime_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        DATE_DIFF('day', MIN(order_date), MAX(order_date)) as customer_lifetime_days
    FROM lakehouse.sales
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    lifetime_value,
    first_order_date,
    last_order_date,
    customer_lifetime_days,
    CASE
        WHEN customer_lifetime_days > 0
        THEN lifetime_value / CAST(customer_lifetime_days AS DOUBLE) * 365
        ELSE lifetime_value
    END as annualized_value,
    CASE
        WHEN total_orders >= 10 THEN 'High Value'
        WHEN total_orders >= 5 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM customer_metrics
WHERE lifetime_value > 0
ORDER BY lifetime_value DESC
LIMIT 100;


-- Product Performance Analysis
SELECT
    p.product_name,
    p.category,
    COUNT(DISTINCT s.order_id) as order_count,
    SUM(s.quantity) as units_sold,
    SUM(s.amount) as total_revenue,
    AVG(s.amount) as avg_order_value,
    SUM(s.quantity * p.cost) as total_cost,
    SUM(s.amount) - SUM(s.quantity * p.cost) as total_profit,
    (SUM(s.amount) - SUM(s.quantity * p.cost)) / NULLIF(SUM(s.amount), 0) * 100 as profit_margin_pct
FROM lakehouse.sales s
JOIN lakehouse.dim_product_current p ON s.product_id = p.product_id
WHERE s.order_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 20;


-- Cohort Analysis
WITH customer_cohorts AS (
    SELECT
        customer_id,
        DATE_FORMAT(MIN(order_date), '%Y-%m') as cohort_month,
        DATE_FORMAT(order_date, '%Y-%m') as order_month,
        SUM(amount) as order_amount
    FROM lakehouse.sales
    GROUP BY customer_id, DATE_FORMAT(order_date, '%Y-%m')
),
cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_customers
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT
    c.cohort_month,
    c.order_month,
    COUNT(DISTINCT c.customer_id) as active_customers,
    cs.cohort_customers,
    COUNT(DISTINCT c.customer_id) * 100.0 / cs.cohort_customers as retention_rate,
    SUM(c.order_amount) as cohort_revenue
FROM customer_cohorts c
JOIN cohort_size cs ON c.cohort_month = cs.cohort_month
GROUP BY c.cohort_month, c.order_month, cs.cohort_customers
ORDER BY c.cohort_month, c.order_month;


-- Sales Trend Analysis with Moving Averages
WITH daily_sales AS (
    SELECT
        order_date,
        SUM(amount) as daily_revenue,
        COUNT(DISTINCT order_id) as daily_orders,
        COUNT(DISTINCT customer_id) as daily_customers
    FROM lakehouse.sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY order_date
)
SELECT
    order_date,
    daily_revenue,
    daily_orders,
    daily_customers,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as revenue_7day_ma,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as revenue_30day_ma,
    daily_revenue - LAG(daily_revenue, 7) OVER (ORDER BY order_date) as wow_revenue_change,
    (daily_revenue - LAG(daily_revenue, 7) OVER (ORDER BY order_date))
        / NULLIF(LAG(daily_revenue, 7) OVER (ORDER BY order_date), 0) * 100 as wow_revenue_change_pct
FROM daily_sales
ORDER BY order_date DESC;


-- ============================================================================
-- PART 6: WINDOW FUNCTIONS AND RANKINGS
-- ============================================================================

-- Customer Ranking by Revenue
SELECT
    customer_id,
    SUM(amount) as total_revenue,
    COUNT(DISTINCT order_id) as order_count,
    ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) as revenue_rank,
    PERCENT_RANK() OVER (ORDER BY SUM(amount)) as revenue_percentile,
    NTILE(10) OVER (ORDER BY SUM(amount)) as revenue_decile
FROM lakehouse.sales
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 100;


-- Product Sales Rank by Category
WITH product_sales AS (
    SELECT
        p.category,
        p.product_name,
        SUM(s.amount) as total_sales,
        SUM(s.quantity) as total_units
    FROM lakehouse.sales s
    JOIN lakehouse.dim_product_current p ON s.product_id = p.product_id
    WHERE s.order_date >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY p.category, p.product_name
)
SELECT
    category,
    product_name,
    total_sales,
    total_units,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_sales DESC) as rank_in_category,
    SUM(total_sales) OVER (PARTITION BY category) as category_total_sales,
    total_sales * 100.0 / SUM(total_sales) OVER (PARTITION BY category) as pct_of_category_sales
FROM product_sales
QUALIFY rank_in_category <= 5
ORDER BY category, rank_in_category;


-- ============================================================================
-- PART 7: INCREMENTAL PROCESSING PATTERNS
-- ============================================================================

-- Process only new data since last run (using watermark)
WITH last_processed AS (
    SELECT MAX(processing_timestamp) as last_run
    FROM lakehouse.etl_watermarks
    WHERE table_name = 'sales'
)
SELECT
    s.*,
    CURRENT_TIMESTAMP as processing_timestamp
FROM lakehouse.sales s
CROSS JOIN last_processed lp
WHERE s.created_at > lp.last_run
   OR lp.last_run IS NULL;


-- Incremental aggregation using snapshots
WITH latest_snapshot AS (
    SELECT MAX(snapshot_id) as current_snapshot
    FROM lakehouse."sales$snapshots"
),
previous_snapshot AS (
    SELECT snapshot_id as previous_snapshot
    FROM lakehouse."sales$snapshots"
    ORDER BY committed_at DESC
    OFFSET 1 LIMIT 1
)
SELECT
    order_date,
    COUNT(*) as new_orders,
    SUM(amount) as new_revenue
FROM lakehouse.sales
WHERE snapshot_id > (SELECT previous_snapshot FROM previous_snapshot)
GROUP BY order_date;


-- ============================================================================
-- PART 8: DATA QUALITY QUERIES
-- ============================================================================

-- Check for null values
SELECT
    'customer_id' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_count,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_percentage
FROM lakehouse.sales
UNION ALL
SELECT
    'amount',
    COUNT(*),
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
FROM lakehouse.sales
UNION ALL
SELECT
    'order_date',
    COUNT(*),
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
FROM lakehouse.sales;


-- Check for duplicates
SELECT
    order_id,
    COUNT(*) as duplicate_count
FROM lakehouse.sales
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- Data freshness check
SELECT
    MAX(order_date) as latest_date,
    MAX(created_at) as latest_ingestion,
    DATE_DIFF('hour', MAX(created_at), CURRENT_TIMESTAMP) as hours_since_last_update,
    CASE
        WHEN DATE_DIFF('hour', MAX(created_at), CURRENT_TIMESTAMP) > 24
        THEN 'STALE'
        WHEN DATE_DIFF('hour', MAX(created_at), CURRENT_TIMESTAMP) > 12
        THEN 'WARNING'
        ELSE 'FRESH'
    END as freshness_status
FROM lakehouse.sales;


-- Statistical outlier detection
WITH sales_stats AS (
    SELECT
        AVG(amount) as mean_amount,
        STDDEV(amount) as stddev_amount
    FROM lakehouse.sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
)
SELECT
    s.order_id,
    s.customer_id,
    s.amount,
    ss.mean_amount,
    ss.stddev_amount,
    (s.amount - ss.mean_amount) / NULLIF(ss.stddev_amount, 0) as z_score,
    CASE
        WHEN ABS((s.amount - ss.mean_amount) / NULLIF(ss.stddev_amount, 0)) > 3
        THEN 'OUTLIER'
        ELSE 'NORMAL'
    END as outlier_status
FROM lakehouse.sales s
CROSS JOIN sales_stats ss
WHERE s.order_date >= CURRENT_DATE - INTERVAL '30' DAY
  AND ABS((s.amount - ss.mean_amount) / NULLIF(ss.stddev_amount, 0)) > 3
ORDER BY ABS(z_score) DESC
LIMIT 20;


-- ============================================================================
-- PART 9: PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Use partition pruning for better performance
SELECT
    product_id,
    SUM(amount) as total_sales
FROM lakehouse.sales
WHERE order_date = DATE '2024-01-15'  -- Partition filter
GROUP BY product_id
ORDER BY total_sales DESC;


-- Use predicate pushdown
SELECT
    customer_id,
    order_date,
    amount
FROM lakehouse.sales
WHERE amount > 1000  -- Filter pushed down to file level
  AND order_date >= DATE '2024-01-01';


-- Leverage Iceberg metadata for count queries
SELECT COUNT(*) FROM lakehouse."sales$files";


-- Use column projection (read only needed columns)
SELECT customer_id, amount  -- Only these columns read
FROM lakehouse.sales
WHERE order_date = DATE '2024-01-15';


-- ============================================================================
-- PART 10: ADVANCED ANALYTICS
-- ============================================================================

-- RFM (Recency, Frequency, Monetary) Analysis
WITH customer_rfm AS (
    SELECT
        customer_id,
        DATE_DIFF('day', MAX(order_date), CURRENT_DATE) as recency,
        COUNT(DISTINCT order_id) as frequency,
        SUM(amount) as monetary
    FROM lakehouse.sales
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        recency,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary) as m_score
    FROM customer_rfm
)
SELECT
    customer_id,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score) as rfm_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'Promising'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        ELSE 'Others'
    END as customer_segment
FROM rfm_scores
ORDER BY rfm_score DESC
LIMIT 100;


-- Market Basket Analysis
WITH order_products AS (
    SELECT
        order_id,
        ARRAY_AGG(product_id) as products
    FROM lakehouse.sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY order_id
)
SELECT
    p1.product_id as product_a,
    p2.product_id as product_b,
    COUNT(*) as co_occurrence,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_products) as support_pct
FROM lakehouse.sales p1
JOIN lakehouse.sales p2
    ON p1.order_id = p2.order_id
   AND p1.product_id < p2.product_id
WHERE p1.order_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY p1.product_id, p2.product_id
HAVING COUNT(*) >= 10
ORDER BY co_occurrence DESC
LIMIT 50;


-- ============================================================================
-- END OF DATA MODELING EXAMPLES
-- ============================================================================

-- Summary of key Trino + Iceberg capabilities demonstrated:
-- ✅ Time travel queries (timestamp and version)
-- ✅ Metadata table queries
-- ✅ Dimensional modeling views
-- ✅ Analytical queries (CLV, cohorts, trends)
-- ✅ Window functions and rankings
-- ✅ Incremental processing patterns
-- ✅ Data quality checks
-- ✅ Performance optimization
-- ✅ Advanced analytics (RFM, market basket)
