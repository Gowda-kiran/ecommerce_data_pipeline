-- E-commerce Data Warehouse Schema
-- Star Schema with Fact and Dimension Tables

-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS fact_sales_aggregated CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    registration_date DATE,
    customer_segment VARCHAR(50),
    loyalty_points INTEGER,
    customer_tenure_days INTEGER,
    is_new_customer BOOLEAN,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiration_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_customer_segment ON dim_customer(customer_segment);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(500),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    stock_quantity INTEGER,
    is_in_stock BOOLEAN,
    stock_status VARCHAR(50),
    supplier_id VARCHAR(50),
    weight_kg DECIMAL(10, 2),
    is_active BOOLEAN,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiration_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_product_id ON dim_product(product_id);
CREATE INDEX idx_category ON dim_product(category);
CREATE INDEX idx_brand ON dim_product(brand);

-- Dimension: Date
CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    season VARCHAR(20)
);

CREATE INDEX idx_year_month ON dim_date(year, month);

-- Fact: Sales Transactions
CREATE TABLE fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    date_key DATE REFERENCES dim_date(date_key),
    transaction_date TIMESTAMP,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    shipping_cost DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    net_amount DECIMAL(10, 2),
    gross_amount DECIMAL(10, 2),
    discount_percentage DECIMAL(5, 2),
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    device_type VARCHAR(50),
    is_valid_transaction BOOLEAN,
    transaction_hour INTEGER,
    is_weekend BOOLEAN
);

CREATE INDEX idx_fact_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_product ON fact_sales(product_key);
CREATE INDEX idx_fact_date ON fact_sales(date_key);
CREATE INDEX idx_order_id ON fact_sales(order_id);
CREATE INDEX idx_transaction_date ON fact_sales(transaction_date);

-- Fact: Aggregated Sales (Daily Summary)
CREATE TABLE fact_sales_aggregated (
    agg_key SERIAL PRIMARY KEY,
    date_key DATE REFERENCES dim_date(date_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    transaction_count INTEGER,
    total_quantity INTEGER,
    total_sales_amount DECIMAL(15, 2),
    total_discount DECIMAL(15, 2),
    total_tax DECIMAL(15, 2),
    total_shipping DECIMAL(15, 2),
    total_net_amount DECIMAL(15, 2),
    avg_unit_price DECIMAL(10, 2),
    max_transaction_amount DECIMAL(10, 2),
    min_transaction_amount DECIMAL(10, 2),
    UNIQUE(date_key, customer_key, product_key)
);

CREATE INDEX idx_agg_date ON fact_sales_aggregated(date_key);
CREATE INDEX idx_agg_customer ON fact_sales_aggregated(customer_key);
CREATE INDEX idx_agg_product ON fact_sales_aggregated(product_key);

-- Comments for documentation
COMMENT ON TABLE dim_customer IS 'Customer dimension table with SCD Type 2 support';
COMMENT ON TABLE dim_product IS 'Product dimension table with SCD Type 2 support';
COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis';
COMMENT ON TABLE fact_sales IS 'Fact table containing all sales transactions';
COMMENT ON TABLE fact_sales_aggregated IS 'Pre-aggregated fact table for better query performance';
