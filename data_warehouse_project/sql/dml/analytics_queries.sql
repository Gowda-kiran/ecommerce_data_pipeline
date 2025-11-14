-- ============================================================================
-- Data Warehouse Analytics Queries
-- Comprehensive set of business intelligence queries
-- ============================================================================

-- ============================================================================
-- 1. SALES PERFORMANCE ANALYSIS
-- ============================================================================

-- 1.1 Daily Sales Summary
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.transaction_id) AS num_transactions,
    COUNT(DISTINCT f.customer_key) AS num_customers,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(100.0 * SUM(f.gross_profit) / NULLIF(SUM(f.total_amount), 0), 2) AS profit_margin_pct,
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.transaction_id), 0), 2) AS avg_transaction_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date DESC;

-- 1.2 Sales Trends by Period
SELECT
    d.year,
    d.quarter,
    d.month_name,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.transaction_id), 0), 2) AS avg_transaction_value,
    -- Year-over-year comparison
    LAG(SUM(f.total_amount)) OVER (
        PARTITION BY d.month_number
        ORDER BY d.year
    ) AS previous_year_revenue,
    ROUND(100.0 * (
        SUM(f.total_amount) -
        LAG(SUM(f.total_amount)) OVER (PARTITION BY d.month_number ORDER BY d.year)
    ) / NULLIF(LAG(SUM(f.total_amount)) OVER (PARTITION BY d.month_number ORDER BY d.year), 0), 2) AS yoy_growth_pct
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month_name, d.month_number
ORDER BY d.year DESC, d.quarter, d.month_number;

-- ============================================================================
-- 2. CUSTOMER ANALYTICS
-- ============================================================================

-- 2.1 Customer Lifetime Value (Top 100)
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_segment,
    c.city,
    c.state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_amount) AS lifetime_value,
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value,
    SUM(f.quantity) AS total_items_purchased,
    MIN(d.full_date) AS first_purchase_date,
    MAX(d.full_date) AS last_purchase_date,
    CURRENT_DATE - MAX(d.full_date) AS days_since_last_purchase,
    MAX(d.full_date) - MIN(d.full_date) AS customer_lifetime_days,
    ROUND(COUNT(DISTINCT f.order_id) * 365.0 / NULLIF(MAX(d.full_date) - MIN(d.full_date), 0), 2) AS annual_order_frequency
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = TRUE
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.customer_id, c.full_name, c.email, c.customer_segment, c.city, c.state
ORDER BY lifetime_value DESC
LIMIT 100;

-- 2.2 Customer Segmentation (RFM Analysis)
WITH customer_rfm AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.full_name,
        c.customer_segment,
        -- Recency: Days since last purchase
        CURRENT_DATE - MAX(d.full_date) AS recency_days,
        -- Frequency: Number of orders
        COUNT(DISTINCT f.order_id) AS frequency,
        -- Monetary: Total spending
        SUM(f.total_amount) AS monetary
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = TRUE
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY c.customer_key, c.customer_id, c.full_name, c.customer_segment
),
rfm_scores AS (
    SELECT
        *,
        -- Calculate quintiles for each metric
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,  -- Lower recency is better
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS monetary_score
    FROM customer_rfm
)
SELECT
    customer_id,
    full_name,
    customer_segment,
    recency_days,
    frequency,
    ROUND(monetary, 2) AS monetary_value,
    recency_score,
    frequency_score,
    monetary_score,
    CONCAT(recency_score, frequency_score, monetary_score) AS rfm_score,
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
        WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Promising'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'Potential Loyalists'
        WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cannot Lose Them'
        WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score <= 2 THEN 'Hibernating'
        ELSE 'Lost'
    END AS customer_category
FROM rfm_scores
ORDER BY monetary_score DESC, frequency_score DESC, recency_score DESC;

-- 2.3 Customer Churn Analysis
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) AS total_customers,
    COUNT(DISTINCT CASE WHEN CURRENT_DATE - MAX(d.full_date) > 90 THEN c.customer_key END) AS churned_customers,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN CURRENT_DATE - MAX(d.full_date) > 90 THEN c.customer_key END) /
        NULLIF(COUNT(DISTINCT c.customer_key), 0), 2) AS churn_rate_pct,
    ROUND(AVG(CURRENT_DATE - MAX(d.full_date)), 2) AS avg_days_since_last_purchase
FROM dim_customer c
LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.customer_segment
ORDER BY churn_rate_pct DESC;

-- ============================================================================
-- 3. PRODUCT PERFORMANCE
-- ============================================================================

-- 3.1 Top Products by Revenue
SELECT
    p.category,
    p.product_name,
    p.brand,
    p.sku,
    SUM(f.quantity) AS units_sold,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(100.0 * SUM(f.gross_profit) / NULLIF(SUM(f.total_amount), 0), 2) AS profit_margin_pct,
    ROUND(SUM(f.total_amount) / NULLIF(SUM(f.quantity), 0), 2) AS avg_selling_price,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.total_amount) DESC) AS rank_in_category
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key AND p.is_current = TRUE
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY p.category, p.product_name, p.brand, p.sku
HAVING SUM(f.quantity) > 0
ORDER BY total_revenue DESC
LIMIT 50;

-- 3.2 Product Category Performance
SELECT
    p.category,
    COUNT(DISTINCT p.product_key) AS num_products,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(100.0 * SUM(f.gross_profit) / NULLIF(SUM(f.total_amount), 0), 2) AS profit_margin_pct,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(DISTINCT f.transaction_id) AS num_transactions,
    ROUND(SUM(f.total_amount) / NULLIF(SUM(f.quantity), 0), 2) AS avg_price_per_unit,
    -- Market share within warehouse
    ROUND(100.0 * SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (), 2) AS revenue_share_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key AND p.is_current = TRUE
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY p.category
ORDER BY total_revenue DESC;

-- ============================================================================
-- 4. INVENTORY ANALYSIS
-- ============================================================================

-- 4.1 Current Inventory Status
SELECT
    p.category,
    p.product_name,
    p.sku,
    s.store_name,
    i.quantity_on_hand,
    i.quantity_on_order,
    i.quantity_available,
    i.reorder_point,
    i.stock_status,
    i.unit_cost,
    i.total_inventory_value,
    i.days_since_last_sale,
    CASE
        WHEN i.quantity_on_hand = 0 THEN 'CRITICAL - Out of Stock'
        WHEN i.quantity_on_hand < i.reorder_point THEN 'WARNING - Below Reorder Point'
        WHEN i.quantity_on_hand > i.reorder_point * 3 THEN 'ATTENTION - Overstock'
        ELSE 'OK'
    END AS inventory_alert
FROM fact_inventory_snapshot i
JOIN dim_product p ON i.product_key = p.product_key AND p.is_current = TRUE
JOIN dim_store s ON i.store_key = s.store_key AND s.is_current = TRUE
JOIN dim_date d ON i.snapshot_date_key = d.date_key
WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date WHERE full_date <= CURRENT_DATE)
  AND p.is_active = TRUE
ORDER BY i.total_inventory_value DESC;

-- 4.2 Inventory Turnover Rate
WITH inventory_metrics AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        AVG(i.quantity_on_hand) AS avg_inventory,
        SUM(s.quantity) AS total_units_sold
    FROM dim_product p
    JOIN fact_inventory_snapshot i ON p.product_key = i.product_key
    JOIN fact_sales s ON p.product_key = s.product_key
    JOIN dim_date d ON i.snapshot_date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '90 days'
      AND p.is_current = TRUE
    GROUP BY p.product_id, p.product_name, p.category
)
SELECT
    product_id,
    product_name,
    category,
    ROUND(avg_inventory, 0) AS avg_inventory_units,
    total_units_sold,
    ROUND(total_units_sold / NULLIF(avg_inventory, 0), 2) AS inventory_turnover_ratio,
    ROUND(90.0 * avg_inventory / NULLIF(total_units_sold, 0), 1) AS days_of_supply,
    CASE
        WHEN total_units_sold / NULLIF(avg_inventory, 0) > 4 THEN 'High Turnover'
        WHEN total_units_sold / NULLIF(avg_inventory, 0) > 2 THEN 'Moderate Turnover'
        WHEN total_units_sold / NULLIF(avg_inventory, 0) > 1 THEN 'Low Turnover'
        ELSE 'Very Low Turnover'
    END AS turnover_category
FROM inventory_metrics
WHERE avg_inventory > 0
ORDER BY inventory_turnover_ratio DESC
LIMIT 100;

-- ============================================================================
-- 5. STORE PERFORMANCE
-- ============================================================================

-- 5.1 Store Sales Performance
SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.state,
    s.region,
    COUNT(DISTINCT f.transaction_id) AS num_transactions,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.gross_profit) AS total_profit,
    ROUND(100.0 * SUM(f.gross_profit) / NULLIF(SUM(f.total_amount), 0), 2) AS profit_margin_pct,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.transaction_id), 0), 2) AS avg_transaction_value,
    RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS revenue_rank
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key AND s.is_current = TRUE
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
  AND s.is_active = TRUE
GROUP BY s.store_id, s.store_name, s.city, s.state, s.region
ORDER BY total_revenue DESC;

-- ============================================================================
-- 6. ORDER FULFILLMENT ANALYSIS
-- ============================================================================

-- 6.1 Fulfillment Performance Metrics
SELECT
    DATE_TRUNC('week', d.full_date) AS week_start,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.is_completed THEN 1 END) AS completed_orders,
    COUNT(CASE WHEN o.is_cancelled THEN 1 END) AS cancelled_orders,
    ROUND(100.0 * COUNT(CASE WHEN o.is_completed THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS completion_rate_pct,
    ROUND(AVG(o.total_fulfillment_hours), 1) AS avg_fulfillment_hours,
    ROUND(AVG(o.hours_to_ship), 1) AS avg_hours_to_ship,
    ROUND(AVG(o.hours_to_delivery), 1) AS avg_hours_to_delivery,
    COUNT(CASE WHEN o.is_on_time THEN 1 END) AS on_time_deliveries,
    ROUND(100.0 * COUNT(CASE WHEN o.is_on_time THEN 1 END) / NULLIF(COUNT(CASE WHEN o.is_completed THEN 1 END), 0), 2) AS on_time_rate_pct
FROM fact_order_fulfillment o
JOIN dim_date d ON o.order_date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_TRUNC('week', d.full_date)
ORDER BY week_start DESC;

-- ============================================================================
-- 7. COHORT ANALYSIS
-- ============================================================================

-- 7.1 Customer Cohort Retention
WITH customer_cohorts AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', MIN(d.full_date)) AS cohort_month
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_id
),
cohort_data AS (
    SELECT
        cc.cohort_month,
        DATE_TRUNC('month', d.full_date) AS activity_month,
        EXTRACT(YEAR FROM AGE(d.full_date, cc.cohort_month::date)) * 12 +
        EXTRACT(MONTH FROM AGE(d.full_date, cc.cohort_month::date)) AS months_since_first_purchase,
        COUNT(DISTINCT f.customer_id) AS active_customers
    FROM customer_cohorts cc
    JOIN dim_customer c ON cc.customer_id = c.customer_id
    JOIN fact_sales f ON c.customer_key = f.customer_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE cc.cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
    GROUP BY cc.cohort_month, activity_month
)
SELECT
    cohort_month,
    months_since_first_purchase AS months_since_first,
    active_customers,
    FIRST_VALUE(active_customers) OVER (
        PARTITION BY cohort_month
        ORDER BY months_since_first_purchase
    ) AS cohort_size,
    ROUND(100.0 * active_customers / FIRST_VALUE(active_customers) OVER (
        PARTITION BY cohort_month
        ORDER BY months_since_first_purchase
    ), 2) AS retention_rate_pct
FROM cohort_data
ORDER BY cohort_month DESC, months_since_first_purchase;

-- ============================================================================
-- 8. EXECUTIVE DASHBOARD KPIs
-- ============================================================================

-- 8.1 Key Performance Indicators (Last 30 Days vs Previous 30 Days)
WITH current_period AS (
    SELECT
        SUM(f.total_amount) AS revenue,
        SUM(f.gross_profit) AS profit,
        COUNT(DISTINCT f.transaction_id) AS transactions,
        COUNT(DISTINCT f.customer_key) AS customers,
        SUM(f.quantity) AS units_sold
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
),
previous_period AS (
    SELECT
        SUM(f.total_amount) AS revenue,
        SUM(f.gross_profit) AS profit,
        COUNT(DISTINCT f.transaction_id) AS transactions,
        COUNT(DISTINCT f.customer_key) AS customers,
        SUM(f.quantity) AS units_sold
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '60 days'
      AND d.full_date < CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    'Revenue' AS metric,
    ROUND(c.revenue, 2) AS current_period,
    ROUND(p.revenue, 2) AS previous_period,
    ROUND(100.0 * (c.revenue - p.revenue) / NULLIF(p.revenue, 0), 2) AS change_pct
FROM current_period c, previous_period p
UNION ALL
SELECT
    'Profit',
    ROUND(c.profit, 2),
    ROUND(p.profit, 2),
    ROUND(100.0 * (c.profit - p.profit) / NULLIF(p.profit, 0), 2)
FROM current_period c, previous_period p
UNION ALL
SELECT
    'Transactions',
    c.transactions,
    p.transactions,
    ROUND(100.0 * (c.transactions - p.transactions) / NULLIF(p.transactions, 0), 2)
FROM current_period c, previous_period p
UNION ALL
SELECT
    'Customers',
    c.customers,
    p.customers,
    ROUND(100.0 * (c.customers - p.customers) / NULLIF(p.customers, 0), 2)
FROM current_period c, previous_period p
UNION ALL
SELECT
    'Units Sold',
    c.units_sold,
    p.units_sold,
    ROUND(100.0 * (c.units_sold - p.units_sold) / NULLIF(p.units_sold, 0), 2)
FROM current_period c, previous_period p;
