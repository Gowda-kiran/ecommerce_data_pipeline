-- ============================================================================
-- Data Warehouse Fact Tables
-- Implements Transactional, Periodic Snapshot, and Accumulating Snapshot Facts
-- ============================================================================

-- ============================================================================
-- 1. FACT_SALES (Transactional Fact Table)
-- Grain: One row per line item per transaction
-- ============================================================================
DROP TABLE IF EXISTS fact_sales CASCADE;

CREATE TABLE fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys to dimensions
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    store_key INTEGER NOT NULL REFERENCES dim_store(store_key),
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    promotion_key INTEGER REFERENCES dim_promotion(promotion_key),
    payment_method_key INTEGER REFERENCES dim_payment_method(payment_method_key),
    transaction_details_key INTEGER REFERENCES dim_transaction_details(transaction_details_key),

    -- Degenerate dimensions (transaction identifiers)
    transaction_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    line_item_number SMALLINT NOT NULL,

    -- Transaction timestamp
    transaction_timestamp TIMESTAMP NOT NULL,
    transaction_time TIME NOT NULL,

    -- Quantity metrics
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    returned_quantity INTEGER DEFAULT 0,
    net_quantity INTEGER NOT NULL,

    -- Price metrics
    unit_list_price DECIMAL(10,2) NOT NULL,
    unit_sale_price DECIMAL(10,2) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,

    -- Amount metrics
    gross_amount DECIMAL(12,2) NOT NULL, -- quantity * unit_list_price
    discount_amount DECIMAL(12,2) DEFAULT 0,
    net_amount DECIMAL(12,2) NOT NULL, -- gross_amount - discount_amount
    tax_amount DECIMAL(12,2) DEFAULT 0,
    shipping_amount DECIMAL(12,2) DEFAULT 0,
    total_amount DECIMAL(12,2) NOT NULL, -- net_amount + tax_amount + shipping_amount

    -- Cost and profit metrics
    total_cost DECIMAL(12,2) NOT NULL, -- quantity * unit_cost
    gross_profit DECIMAL(12,2) NOT NULL, -- net_amount - total_cost
    profit_margin_percent DECIMAL(5,2),

    -- Additional metrics
    loyalty_points_earned INTEGER DEFAULT 0,
    loyalty_points_redeemed INTEGER DEFAULT 0,

    -- Flags
    is_return BOOLEAN DEFAULT FALSE,
    is_exchange BOOLEAN DEFAULT FALSE,
    is_voided BOOLEAN DEFAULT FALSE,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),
    source_system VARCHAR(50),

    -- Constraints
    CONSTRAINT chk_sales_amounts CHECK (total_amount >= 0),
    CONSTRAINT chk_sales_quantity CHECK (net_quantity >= 0),
    CONSTRAINT uq_sales_transaction_line UNIQUE (transaction_id, line_item_number)
) PARTITION BY RANGE (date_key);

-- Create partitions for fact_sales (example for 2024)
CREATE TABLE fact_sales_2024_q1 PARTITION OF fact_sales
    FOR VALUES FROM (20240101) TO (20240401);

CREATE TABLE fact_sales_2024_q2 PARTITION OF fact_sales
    FOR VALUES FROM (20240401) TO (20240701);

CREATE TABLE fact_sales_2024_q3 PARTITION OF fact_sales
    FOR VALUES FROM (20240701) TO (20241001);

CREATE TABLE fact_sales_2024_q4 PARTITION OF fact_sales
    FOR VALUES FROM (20241001) TO (20250101);

-- Indexes on fact_sales
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX idx_fact_sales_transaction ON fact_sales(transaction_id);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_timestamp ON fact_sales(transaction_timestamp);
CREATE INDEX idx_fact_sales_composite ON fact_sales(date_key, customer_key, product_key);

COMMENT ON TABLE fact_sales IS 'Transactional fact table for sales - grain: one row per line item';

-- ============================================================================
-- 2. FACT_INVENTORY_SNAPSHOT (Periodic Snapshot Fact Table)
-- Grain: One row per product per store per day
-- ============================================================================
DROP TABLE IF EXISTS fact_inventory_snapshot CASCADE;

CREATE TABLE fact_inventory_snapshot (
    inventory_snapshot_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    snapshot_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    store_key INTEGER NOT NULL REFERENCES dim_store(store_key),

    -- Inventory quantities
    quantity_on_hand INTEGER NOT NULL DEFAULT 0,
    quantity_on_order INTEGER DEFAULT 0,
    quantity_in_transit INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    quantity_available INTEGER NOT NULL,

    -- Inventory values
    unit_cost DECIMAL(10,2) NOT NULL,
    total_inventory_value DECIMAL(12,2) NOT NULL,

    -- Reorder metrics
    reorder_point INTEGER NOT NULL,
    reorder_quantity INTEGER NOT NULL,
    days_of_supply INTEGER,

    -- Movement metrics (changes from previous day)
    quantity_received INTEGER DEFAULT 0,
    quantity_sold INTEGER DEFAULT 0,
    quantity_adjusted INTEGER DEFAULT 0,
    quantity_damaged INTEGER DEFAULT 0,
    quantity_returned INTEGER DEFAULT 0,

    -- Stock status
    stock_status VARCHAR(50), -- In Stock, Low Stock, Out of Stock, Overstock
    is_out_of_stock BOOLEAN DEFAULT FALSE,
    is_below_reorder_point BOOLEAN DEFAULT FALSE,
    is_overstock BOOLEAN DEFAULT FALSE,

    -- Aging analysis
    days_since_last_sale INTEGER,
    inventory_turnover_rate DECIMAL(10,2),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),

    -- Ensure one snapshot per product per store per day
    CONSTRAINT uq_inventory_snapshot UNIQUE (snapshot_date_key, product_key, store_key)
) PARTITION BY RANGE (snapshot_date_key);

-- Create partitions
CREATE TABLE fact_inventory_snapshot_2024_q1 PARTITION OF fact_inventory_snapshot
    FOR VALUES FROM (20240101) TO (20240401);

CREATE TABLE fact_inventory_snapshot_2024_q2 PARTITION OF fact_inventory_snapshot
    FOR VALUES FROM (20240401) TO (20240701);

CREATE TABLE fact_inventory_snapshot_2024_q3 PARTITION OF fact_inventory_snapshot
    FOR VALUES FROM (20240701) TO (20241001);

CREATE TABLE fact_inventory_snapshot_2024_q4 PARTITION OF fact_inventory_snapshot
    FOR VALUES FROM (20241001) TO (20250101);

-- Indexes
CREATE INDEX idx_fact_inventory_date ON fact_inventory_snapshot(snapshot_date_key);
CREATE INDEX idx_fact_inventory_product ON fact_inventory_snapshot(product_key);
CREATE INDEX idx_fact_inventory_store ON fact_inventory_snapshot(store_key);
CREATE INDEX idx_fact_inventory_status ON fact_inventory_snapshot(stock_status);
CREATE INDEX idx_fact_inventory_composite ON fact_inventory_snapshot(snapshot_date_key, store_key);

COMMENT ON TABLE fact_inventory_snapshot IS 'Periodic snapshot fact for daily inventory positions';

-- ============================================================================
-- 3. FACT_ORDER_FULFILLMENT (Accumulating Snapshot Fact Table)
-- Grain: One row per order (updated as order progresses through pipeline)
-- ============================================================================
DROP TABLE IF EXISTS fact_order_fulfillment CASCADE;

CREATE TABLE fact_order_fulfillment (
    order_fulfillment_key BIGSERIAL PRIMARY KEY,

    -- Order identification
    order_id VARCHAR(100) NOT NULL UNIQUE,

    -- Foreign keys to dimensions (some may be NULL initially)
    order_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    store_key INTEGER REFERENCES dim_store(store_key),

    -- Milestone date keys (foreign keys to date dimension)
    order_received_date_key INTEGER REFERENCES dim_date(date_key),
    payment_confirmed_date_key INTEGER REFERENCES dim_date(date_key),
    order_validated_date_key INTEGER REFERENCES dim_date(date_key),
    picking_started_date_key INTEGER REFERENCES dim_date(date_key),
    picking_completed_date_key INTEGER REFERENCES dim_date(date_key),
    packing_completed_date_key INTEGER REFERENCES dim_date(date_key),
    shipped_date_key INTEGER REFERENCES dim_date(date_key),
    out_for_delivery_date_key INTEGER REFERENCES dim_date(date_key),
    delivered_date_key INTEGER REFERENCES dim_date(date_key),

    -- Actual milestone timestamps
    order_received_timestamp TIMESTAMP NOT NULL,
    payment_confirmed_timestamp TIMESTAMP,
    order_validated_timestamp TIMESTAMP,
    picking_started_timestamp TIMESTAMP,
    picking_completed_timestamp TIMESTAMP,
    packing_completed_timestamp TIMESTAMP,
    shipped_timestamp TIMESTAMP,
    out_for_delivery_timestamp TIMESTAMP,
    delivered_timestamp TIMESTAMP,

    -- Duration metrics (in hours)
    hours_to_payment INTEGER,
    hours_to_validation INTEGER,
    hours_to_picking_start INTEGER,
    hours_to_picking_complete INTEGER,
    hours_to_packing_complete INTEGER,
    hours_to_ship INTEGER,
    hours_to_delivery INTEGER,
    total_fulfillment_hours INTEGER,

    -- Order metrics
    total_items INTEGER NOT NULL,
    total_order_amount DECIMAL(12,2) NOT NULL,
    shipping_cost DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    final_amount DECIMAL(12,2) NOT NULL,

    -- Current status
    current_status VARCHAR(50) NOT NULL,
    is_completed BOOLEAN DEFAULT FALSE,
    is_cancelled BOOLEAN DEFAULT FALSE,
    is_returned BOOLEAN DEFAULT FALSE,

    -- Cancellation/return info
    cancellation_date_key INTEGER REFERENCES dim_date(date_key),
    cancellation_reason VARCHAR(255),
    return_date_key INTEGER REFERENCES dim_date(date_key),
    return_reason VARCHAR(255),

    -- Performance flags
    is_on_time BOOLEAN,
    is_delayed BOOLEAN,
    delay_hours INTEGER DEFAULT 0,

    -- Shipping details
    shipping_method VARCHAR(50),
    carrier VARCHAR(100),
    tracking_number VARCHAR(100),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_stage VARCHAR(50),
    version INTEGER DEFAULT 1
);

-- Indexes
CREATE INDEX idx_fact_order_fulfillment_order_id ON fact_order_fulfillment(order_id);
CREATE INDEX idx_fact_order_fulfillment_customer ON fact_order_fulfillment(customer_key);
CREATE INDEX idx_fact_order_fulfillment_status ON fact_order_fulfillment(current_status);
CREATE INDEX idx_fact_order_fulfillment_order_date ON fact_order_fulfillment(order_date_key);
CREATE INDEX idx_fact_order_fulfillment_delivered ON fact_order_fulfillment(delivered_date_key);
CREATE INDEX idx_fact_order_fulfillment_updated ON fact_order_fulfillment(updated_at);

COMMENT ON TABLE fact_order_fulfillment IS 'Accumulating snapshot fact for order pipeline tracking';

-- ============================================================================
-- 4. FACT_CUSTOMER_SNAPSHOT (Periodic Snapshot Fact Table)
-- Grain: One row per customer per month
-- ============================================================================
DROP TABLE IF EXISTS fact_customer_snapshot CASCADE;

CREATE TABLE fact_customer_snapshot (
    customer_snapshot_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    snapshot_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),

    -- Customer metrics for the month
    num_orders INTEGER DEFAULT 0,
    num_items_purchased INTEGER DEFAULT 0,
    total_amount_spent DECIMAL(12,2) DEFAULT 0,
    total_discounts_received DECIMAL(12,2) DEFAULT 0,
    total_returns INTEGER DEFAULT 0,
    return_rate DECIMAL(5,2),

    -- Lifetime metrics (cumulative)
    lifetime_orders INTEGER DEFAULT 0,
    lifetime_amount_spent DECIMAL(12,2) DEFAULT 0,
    lifetime_items_purchased INTEGER DEFAULT 0,

    -- Loyalty metrics
    loyalty_points_balance INTEGER DEFAULT 0,
    loyalty_points_earned_mtd INTEGER DEFAULT 0,
    loyalty_points_redeemed_mtd INTEGER DEFAULT 0,
    loyalty_tier VARCHAR(50),

    -- Engagement metrics
    days_since_last_purchase INTEGER,
    average_order_value DECIMAL(12,2),
    purchase_frequency DECIMAL(10,2),

    -- Customer status
    is_active_customer BOOLEAN DEFAULT TRUE,
    is_at_risk BOOLEAN DEFAULT FALSE,
    churn_risk_score DECIMAL(5,2),

    -- RFM segmentation
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    rfm_segment VARCHAR(50),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_customer_snapshot UNIQUE (snapshot_date_key, customer_key)
) PARTITION BY RANGE (snapshot_date_key);

-- Create partitions
CREATE TABLE fact_customer_snapshot_2024 PARTITION OF fact_customer_snapshot
    FOR VALUES FROM (20240101) TO (20250101);

-- Indexes
CREATE INDEX idx_fact_customer_snapshot_date ON fact_customer_snapshot(snapshot_date_key);
CREATE INDEX idx_fact_customer_snapshot_customer ON fact_customer_snapshot(customer_key);
CREATE INDEX idx_fact_customer_snapshot_rfm ON fact_customer_snapshot(rfm_segment);

COMMENT ON TABLE fact_customer_snapshot IS 'Monthly snapshot of customer metrics and behavior';

-- ============================================================================
-- 5. FACT_PRODUCT_PERFORMANCE (Aggregate Fact Table)
-- Grain: One row per product per day (pre-aggregated for performance)
-- ============================================================================
DROP TABLE IF EXISTS fact_product_performance CASCADE;

CREATE TABLE fact_product_performance (
    product_performance_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),

    -- Sales metrics
    units_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0,
    total_cost DECIMAL(12,2) DEFAULT 0,
    total_profit DECIMAL(12,2) DEFAULT 0,
    average_selling_price DECIMAL(10,2),

    -- Transaction metrics
    num_transactions INTEGER DEFAULT 0,
    num_customers INTEGER DEFAULT 0,
    num_stores_sold_at INTEGER DEFAULT 0,

    -- Discount metrics
    total_discount_amount DECIMAL(12,2) DEFAULT 0,
    discount_rate DECIMAL(5,2),

    -- Return metrics
    units_returned INTEGER DEFAULT 0,
    return_rate DECIMAL(5,2),
    return_amount DECIMAL(12,2) DEFAULT 0,

    -- Inventory impact
    ending_inventory INTEGER,
    days_of_inventory INTEGER,

    -- Ranking metrics
    sales_rank_category INTEGER,
    sales_rank_overall INTEGER,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_product_performance UNIQUE (date_key, product_key)
) PARTITION BY RANGE (date_key);

-- Create partitions
CREATE TABLE fact_product_performance_2024_q1 PARTITION OF fact_product_performance
    FOR VALUES FROM (20240101) TO (20240401);

CREATE TABLE fact_product_performance_2024_q2 PARTITION OF fact_product_performance
    FOR VALUES FROM (20240401) TO (20240701);

CREATE TABLE fact_product_performance_2024_q3 PARTITION OF fact_product_performance
    FOR VALUES FROM (20240701) TO (20241001);

CREATE TABLE fact_product_performance_2024_q4 PARTITION OF fact_product_performance
    FOR VALUES FROM (20241001) TO (20250101);

-- Indexes
CREATE INDEX idx_fact_product_perf_date ON fact_product_performance(date_key);
CREATE INDEX idx_fact_product_perf_product ON fact_product_performance(product_key);
CREATE INDEX idx_fact_product_perf_revenue ON fact_product_performance(total_revenue DESC);

COMMENT ON TABLE fact_product_performance IS 'Daily aggregated product performance metrics';

-- ============================================================================
-- Fact Tables Summary
-- ============================================================================
SELECT 'Fact tables created successfully' AS status;

SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename LIKE 'fact_%'
ORDER BY tablename;

-- Display table relationships
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name LIKE 'fact_%'
ORDER BY tc.table_name, kcu.column_name;
