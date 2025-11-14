-- ============================================================================
-- Data Warehouse Dimension Tables
-- Implements SCD Types 1, 2, and 3
-- ============================================================================

-- ============================================================================
-- 1. DATE DIMENSION (SCD Type 1 - No History)
-- ============================================================================
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,

    -- Date components
    day_of_month SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_name_short VARCHAR(3) NOT NULL,

    -- Week
    week_of_year SMALLINT NOT NULL,
    week_of_month SMALLINT NOT NULL,

    -- Month
    month_number SMALLINT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    month_name_short VARCHAR(3) NOT NULL,

    -- Quarter
    quarter SMALLINT NOT NULL,
    quarter_name VARCHAR(2) NOT NULL,

    -- Year
    year SMALLINT NOT NULL,

    -- Fiscal calendar (assuming fiscal year starts in April)
    fiscal_year SMALLINT NOT NULL,
    fiscal_quarter SMALLINT NOT NULL,
    fiscal_month SMALLINT NOT NULL,

    -- Flags
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    is_weekday BOOLEAN NOT NULL,
    is_last_day_of_month BOOLEAN NOT NULL,

    -- Holiday name (if applicable)
    holiday_name VARCHAR(50),

    -- Season
    season VARCHAR(10) NOT NULL,

    -- Relative dates
    days_from_today INTEGER,
    weeks_from_today INTEGER,
    months_from_today INTEGER,
    quarters_from_today INTEGER,
    years_from_today INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month_number);
CREATE INDEX idx_dim_date_fiscal_year ON dim_date(fiscal_year);

COMMENT ON TABLE dim_date IS 'Date dimension for temporal analysis';

-- ============================================================================
-- 2. CUSTOMER DIMENSION (SCD Type 2 - Historical Tracking)
-- ============================================================================
DROP TABLE IF EXISTS dim_customer CASCADE;

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,

    -- Customer details
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),

    -- Address
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),

    -- Demographics
    date_of_birth DATE,
    age_bracket VARCHAR(20),
    gender VARCHAR(20),

    -- Customer attributes
    customer_segment VARCHAR(50),
    credit_rating VARCHAR(20),
    preferred_contact_method VARCHAR(20),
    marketing_opt_in BOOLEAN,

    -- Registration info
    registration_date DATE,
    registration_channel VARCHAR(50),

    -- Loyalty program
    loyalty_member BOOLEAN,
    loyalty_tier VARCHAR(20),
    loyalty_points INTEGER,

    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version INTEGER NOT NULL DEFAULT 1,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'ETL_PROCESS',
    updated_by VARCHAR(100) DEFAULT 'ETL_PROCESS',

    -- Constraint to ensure only one current record per customer
    CONSTRAINT uq_customer_current UNIQUE (customer_id, is_current)
);

-- Indexes
CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON dim_customer(customer_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_customer_email ON dim_customer(email);
CREATE INDEX idx_dim_customer_segment ON dim_customer(customer_segment);
CREATE INDEX idx_dim_customer_effective_date ON dim_customer(effective_date);

COMMENT ON TABLE dim_customer IS 'Customer dimension with SCD Type 2 for historical tracking';

-- ============================================================================
-- 3. PRODUCT DIMENSION (SCD Type 2 - Historical Tracking)
-- ============================================================================
DROP TABLE IF EXISTS dim_product CASCADE;

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,

    -- Product identification
    sku VARCHAR(50),
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    brand VARCHAR(100),

    -- Classification
    category VARCHAR(100),
    subcategory VARCHAR(100),
    department VARCHAR(100),
    product_line VARCHAR(100),

    -- Pricing
    list_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    wholesale_price DECIMAL(10,2),
    profit_margin DECIMAL(5,2),

    -- Product attributes
    size VARCHAR(50),
    color VARCHAR(50),
    weight_kg DECIMAL(10,2),
    dimensions VARCHAR(100),

    -- Supplier
    supplier_id VARCHAR(50),
    supplier_name VARCHAR(255),

    -- Inventory management
    reorder_level INTEGER,
    reorder_quantity INTEGER,
    lead_time_days INTEGER,

    -- Product status
    is_active BOOLEAN DEFAULT TRUE,
    is_discontinued BOOLEAN DEFAULT FALSE,
    discontinuation_date DATE,

    -- Launch information
    introduction_date DATE,

    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version INTEGER NOT NULL DEFAULT 1,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'ETL_PROCESS',
    updated_by VARCHAR(100) DEFAULT 'ETL_PROCESS',

    CONSTRAINT uq_product_current UNIQUE (product_id, is_current)
);

-- Indexes
CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_current ON dim_product(product_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_brand ON dim_product(brand);
CREATE INDEX idx_dim_product_sku ON dim_product(sku);

COMMENT ON TABLE dim_product IS 'Product dimension with SCD Type 2 for price and attribute changes';

-- ============================================================================
-- 4. STORE DIMENSION (SCD Type 2 - Historical Tracking)
-- ============================================================================
DROP TABLE IF EXISTS dim_store CASCADE;

CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,

    -- Store details
    store_name VARCHAR(255) NOT NULL,
    store_number VARCHAR(20),
    store_type VARCHAR(50),  -- Retail, Outlet, Flagship, etc.
    store_format VARCHAR(50), -- Mall, Standalone, etc.

    -- Location
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    region VARCHAR(50),
    territory VARCHAR(50),

    -- Contact
    phone VARCHAR(50),
    email VARCHAR(255),

    -- Store attributes
    square_footage INTEGER,
    parking_spaces INTEGER,

    -- Operating hours
    opening_time TIME,
    closing_time TIME,
    is_24_hours BOOLEAN DEFAULT FALSE,

    -- Management
    store_manager VARCHAR(255),
    district_manager VARCHAR(255),
    regional_manager VARCHAR(255),

    -- Dates
    opening_date DATE,
    closing_date DATE,
    last_remodel_date DATE,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version INTEGER NOT NULL DEFAULT 1,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_store_current UNIQUE (store_id, is_current)
);

-- Indexes
CREATE INDEX idx_dim_store_id ON dim_store(store_id);
CREATE INDEX idx_dim_store_current ON dim_store(store_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_store_city ON dim_store(city);
CREATE INDEX idx_dim_store_region ON dim_store(region);

COMMENT ON TABLE dim_store IS 'Store dimension with SCD Type 2 for location and management changes';

-- ============================================================================
-- 5. EMPLOYEE DIMENSION (SCD Type 3 - Previous Value)
-- ============================================================================
DROP TABLE IF EXISTS dim_employee CASCADE;

CREATE TABLE dim_employee (
    employee_key SERIAL PRIMARY KEY,
    employee_id VARCHAR(50) NOT NULL UNIQUE,

    -- Personal details
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),

    -- Employment details
    hire_date DATE NOT NULL,
    termination_date DATE,
    employment_status VARCHAR(50), -- Active, Terminated, On Leave
    employee_type VARCHAR(50), -- Full-time, Part-time, Contract

    -- Current position (SCD Type 1)
    current_job_title VARCHAR(255),
    current_department VARCHAR(100),
    current_manager_id VARCHAR(50),
    current_manager_name VARCHAR(255),
    current_salary_band VARCHAR(20),

    -- Previous position (SCD Type 3)
    previous_job_title VARCHAR(255),
    previous_department VARCHAR(100),
    previous_manager_id VARCHAR(50),
    previous_manager_name VARCHAR(255),

    -- Change tracking
    position_change_date DATE,
    position_change_reason VARCHAR(255),

    -- Location
    work_location VARCHAR(255),
    work_city VARCHAR(100),
    work_state VARCHAR(50),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    is_active BOOLEAN DEFAULT TRUE
);

-- Indexes
CREATE INDEX idx_dim_employee_id ON dim_employee(employee_id);
CREATE INDEX idx_dim_employee_department ON dim_employee(current_department);
CREATE INDEX idx_dim_employee_manager ON dim_employee(current_manager_id);
CREATE INDEX idx_dim_employee_status ON dim_employee(employment_status);

COMMENT ON TABLE dim_employee IS 'Employee dimension with SCD Type 3 for previous position tracking';

-- ============================================================================
-- 6. PROMOTION DIMENSION (SCD Type 1 - Current Value Only)
-- ============================================================================
DROP TABLE IF EXISTS dim_promotion CASCADE;

CREATE TABLE dim_promotion (
    promotion_key SERIAL PRIMARY KEY,
    promotion_id VARCHAR(50) NOT NULL UNIQUE,

    -- Promotion details
    promotion_name VARCHAR(255) NOT NULL,
    promotion_description TEXT,
    promotion_type VARCHAR(50), -- Discount, BOGO, Free Shipping, etc.

    -- Discount details
    discount_percent DECIMAL(5,2),
    discount_amount DECIMAL(10,2),

    -- Date range
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,

    -- Applicability
    applies_to VARCHAR(50), -- Product, Category, Order, etc.
    minimum_purchase_amount DECIMAL(10,2),
    maximum_discount_amount DECIMAL(10,2),

    -- Rules
    promo_code VARCHAR(50),
    requires_coupon BOOLEAN DEFAULT FALSE,
    stackable BOOLEAN DEFAULT FALSE,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_promotion_id ON dim_promotion(promotion_id);
CREATE INDEX idx_dim_promotion_dates ON dim_promotion(start_date, end_date);
CREATE INDEX idx_dim_promotion_active ON dim_promotion(is_active);
CREATE INDEX idx_dim_promotion_code ON dim_promotion(promo_code);

COMMENT ON TABLE dim_promotion IS 'Promotion dimension for marketing campaigns and discounts';

-- ============================================================================
-- 7. PAYMENT METHOD DIMENSION (SCD Type 1)
-- ============================================================================
DROP TABLE IF EXISTS dim_payment_method CASCADE;

CREATE TABLE dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_id VARCHAR(50) NOT NULL UNIQUE,

    -- Payment method details
    payment_method_name VARCHAR(100) NOT NULL,
    payment_method_type VARCHAR(50), -- Credit Card, Debit Card, PayPal, etc.
    payment_provider VARCHAR(100),

    -- Fees
    processing_fee_percent DECIMAL(5,2),
    processing_fee_flat DECIMAL(10,2),

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_payment_method_id ON dim_payment_method(payment_method_id);
CREATE INDEX idx_dim_payment_method_type ON dim_payment_method(payment_method_type);

COMMENT ON TABLE dim_payment_method IS 'Payment method dimension';

-- ============================================================================
-- 8. JUNK DIMENSION (Multiple Low-Cardinality Attributes)
-- ============================================================================
DROP TABLE IF EXISTS dim_transaction_details CASCADE;

CREATE TABLE dim_transaction_details (
    transaction_details_key SERIAL PRIMARY KEY,

    -- Order flags
    is_online_order BOOLEAN NOT NULL,
    is_express_shipping BOOLEAN NOT NULL,
    is_gift_wrapped BOOLEAN NOT NULL,
    is_gift_card BOOLEAN NOT NULL,

    -- Transaction flags
    has_discount BOOLEAN NOT NULL,
    has_coupon BOOLEAN NOT NULL,
    is_return BOOLEAN NOT NULL,
    is_exchange BOOLEAN NOT NULL,

    -- Customer flags
    is_first_purchase BOOLEAN NOT NULL,
    is_loyalty_member BOOLEAN NOT NULL,

    -- Create a unique combination
    CONSTRAINT uq_transaction_details UNIQUE (
        is_online_order, is_express_shipping, is_gift_wrapped, is_gift_card,
        has_discount, has_coupon, is_return, is_exchange,
        is_first_purchase, is_loyalty_member
    )
);

-- Populate junk dimension with all combinations (2^10 = 1024 rows max)
INSERT INTO dim_transaction_details (
    is_online_order, is_express_shipping, is_gift_wrapped, is_gift_card,
    has_discount, has_coupon, is_return, is_exchange,
    is_first_purchase, is_loyalty_member
)
SELECT
    bool1, bool2, bool3, bool4, bool5, bool6, bool7, bool8, bool9, bool10
FROM
    (SELECT TRUE AS bool_val UNION SELECT FALSE) AS b1(bool1),
    (SELECT TRUE UNION SELECT FALSE) AS b2(bool2),
    (SELECT TRUE UNION SELECT FALSE) AS b3(bool3),
    (SELECT TRUE UNION SELECT FALSE) AS b4(bool4),
    (SELECT TRUE UNION SELECT FALSE) AS b5(bool5),
    (SELECT TRUE UNION SELECT FALSE) AS b6(bool6),
    (SELECT TRUE UNION SELECT FALSE) AS b7(bool7),
    (SELECT TRUE UNION SELECT FALSE) AS b8(bool8),
    (SELECT TRUE UNION SELECT FALSE) AS b9(bool9),
    (SELECT TRUE UNION SELECT FALSE) AS b10(bool10);

COMMENT ON TABLE dim_transaction_details IS 'Junk dimension for low-cardinality transaction flags';

-- ============================================================================
-- Dimension Load Summary
-- ============================================================================
SELECT 'Dimension tables created successfully' AS status;

SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename LIKE 'dim_%'
ORDER BY tablename;
