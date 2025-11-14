-- Sample Analytics Queries for E-commerce Data Warehouse

-- 1. Top 10 Products by Revenue
SELECT
    p.product_name,
    p.category,
    p.brand,
    SUM(f.total_amount) as total_revenue,
    SUM(f.quantity) as total_quantity_sold,
    COUNT(DISTINCT f.order_id) as order_count
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.is_valid_transaction = TRUE
GROUP BY p.product_name, p.category, p.brand
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Monthly Sales Trend
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    SUM(f.quantity) as total_items_sold
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_valid_transaction = TRUE
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 3. Customer Segmentation Analysis
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_value,
    SUM(f.total_amount) / COUNT(DISTINCT c.customer_key) as revenue_per_customer
FROM dim_customer c
LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
WHERE f.is_valid_transaction = TRUE
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- 4. Product Category Performance
SELECT
    p.category,
    COUNT(DISTINCT p.product_key) as product_count,
    SUM(f.total_amount) as total_revenue,
    SUM(f.quantity) as units_sold,
    AVG(p.profit_margin) as avg_profit_margin,
    SUM(f.total_amount) / NULLIF(SUM(f.quantity), 0) as avg_price_per_unit
FROM dim_product p
LEFT JOIN fact_sales f ON p.product_key = f.product_key
WHERE f.is_valid_transaction = TRUE
GROUP BY p.category
ORDER BY total_revenue DESC;

-- 5. Weekend vs Weekday Sales
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_valid_transaction = TRUE
GROUP BY d.is_weekend;

-- 6. Top 20 Customers by Lifetime Value
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_segment,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_amount) as lifetime_value,
    AVG(f.total_amount) as avg_order_value,
    MAX(f.transaction_date) as last_purchase_date
FROM dim_customer c
JOIN fact_sales f ON c.customer_key = f.customer_key
WHERE f.is_valid_transaction = TRUE
GROUP BY c.customer_id, c.full_name, c.email, c.customer_segment
ORDER BY lifetime_value DESC
LIMIT 20;

-- 7. Payment Method Analysis
SELECT
    f.payment_method,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_value,
    SUM(f.total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales WHERE is_valid_transaction = TRUE) as revenue_percentage
FROM fact_sales f
WHERE f.is_valid_transaction = TRUE
GROUP BY f.payment_method
ORDER BY total_revenue DESC;

-- 8. Hourly Sales Pattern
SELECT
    f.transaction_hour,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value
FROM fact_sales f
WHERE f.is_valid_transaction = TRUE
GROUP BY f.transaction_hour
ORDER BY f.transaction_hour;

-- 9. Product Stock Analysis
SELECT
    p.category,
    p.stock_status,
    COUNT(*) as product_count,
    AVG(p.stock_quantity) as avg_stock_quantity,
    SUM(CASE WHEN p.is_active THEN 1 ELSE 0 END) as active_products
FROM dim_product p
WHERE p.is_current = TRUE
GROUP BY p.category, p.stock_status
ORDER BY p.category, p.stock_status;

-- 10. Customer Acquisition by Month
SELECT
    DATE_TRUNC('month', c.registration_date) as registration_month,
    COUNT(*) as new_customers,
    AVG(c.loyalty_points) as avg_loyalty_points
FROM dim_customer c
WHERE c.is_current = TRUE
GROUP BY DATE_TRUNC('month', c.registration_date)
ORDER BY registration_month DESC;

-- 11. Order Status Distribution
SELECT
    f.order_status,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_value,
    COUNT(DISTINCT f.order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM fact_sales) as percentage
FROM fact_sales f
GROUP BY f.order_status
ORDER BY order_count DESC;

-- 12. Device Type Performance
SELECT
    f.device_type,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    SUM(f.quantity) as total_items
FROM fact_sales f
WHERE f.is_valid_transaction = TRUE
GROUP BY f.device_type
ORDER BY total_revenue DESC;
