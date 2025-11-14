-- ============================================================================
-- SCD Type 2 Implementation for Customer Dimension
-- Handles new customers and changes to existing customers
-- ============================================================================

CREATE OR REPLACE FUNCTION load_dim_customer_scd2()
RETURNS TABLE (
    new_customers INT,
    updated_customers INT,
    unchanged_customers INT,
    total_processed INT
) AS $$
DECLARE
    v_new_count INT := 0;
    v_updated_count INT := 0;
    v_unchanged_count INT := 0;
    v_total_count INT;
BEGIN
    -- Create temporary staging table for incoming customer data
    CREATE TEMP TABLE staging_customer (
        customer_id VARCHAR(50),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        phone VARCHAR(50),
        address_line1 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        zip_code VARCHAR(20),
        country VARCHAR(50),
        customer_segment VARCHAR(50),
        loyalty_member BOOLEAN,
        loyalty_tier VARCHAR(20),
        loyalty_points INTEGER
    ) ON COMMIT DROP;

    -- Note: In production, you would load data into staging_customer from source system
    -- For this example, assume staging_customer is already loaded

    -- Get total records to process
    SELECT COUNT(*) INTO v_total_count FROM staging_customer;

    -- ========================================================================
    -- STEP 1: Handle New Customers (not in dimension)
    -- ========================================================================
    INSERT INTO dim_customer (
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        address_line1,
        city,
        state,
        zip_code,
        country,
        customer_segment,
        loyalty_member,
        loyalty_tier,
        loyalty_points,
        effective_date,
        expiration_date,
        is_current,
        version,
        created_at,
        updated_at
    )
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        CONCAT(s.first_name, ' ', s.last_name) AS full_name,
        s.email,
        s.phone,
        s.address_line1,
        s.city,
        s.state,
        s.zip_code,
        s.country,
        s.customer_segment,
        s.loyalty_member,
        s.loyalty_tier,
        s.loyalty_points,
        CURRENT_DATE AS effective_date,
        NULL AS expiration_date,
        TRUE AS is_current,
        1 AS version,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM staging_customer s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_customer d
        WHERE d.customer_id = s.customer_id
    );

    GET DIAGNOSTICS v_new_count = ROW_COUNT;

    -- ========================================================================
    -- STEP 2: Handle Changed Customers (SCD Type 2 attributes changed)
    -- ========================================================================

    -- 2a: Expire the old records (set expiration_date and is_current = FALSE)
    UPDATE dim_customer d
    SET
        expiration_date = CURRENT_DATE - INTERVAL '1 day',
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    FROM staging_customer s
    WHERE d.customer_id = s.customer_id
      AND d.is_current = TRUE
      AND (
          -- Check if any Type 2 attributes have changed
          COALESCE(d.first_name, '') != COALESCE(s.first_name, '') OR
          COALESCE(d.last_name, '') != COALESCE(s.last_name, '') OR
          COALESCE(d.email, '') != COALESCE(s.email, '') OR
          COALESCE(d.phone, '') != COALESCE(s.phone, '') OR
          COALESCE(d.address_line1, '') != COALESCE(s.address_line1, '') OR
          COALESCE(d.city, '') != COALESCE(s.city, '') OR
          COALESCE(d.state, '') != COALESCE(s.state, '') OR
          COALESCE(d.zip_code, '') != COALESCE(s.zip_code, '') OR
          COALESCE(d.customer_segment, '') != COALESCE(s.customer_segment, '')
      );

    GET DIAGNOSTICS v_updated_count = ROW_COUNT;

    -- 2b: Insert new version of changed records
    INSERT INTO dim_customer (
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        address_line1,
        city,
        state,
        zip_code,
        country,
        customer_segment,
        loyalty_member,
        loyalty_tier,
        loyalty_points,
        effective_date,
        expiration_date,
        is_current,
        version,
        created_at,
        updated_at
    )
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        CONCAT(s.first_name, ' ', s.last_name) AS full_name,
        s.email,
        s.phone,
        s.address_line1,
        s.city,
        s.state,
        s.zip_code,
        s.country,
        s.customer_segment,
        s.loyalty_member,
        s.loyalty_tier,
        s.loyalty_points,
        CURRENT_DATE AS effective_date,
        NULL AS expiration_date,
        TRUE AS is_current,
        COALESCE((
            SELECT MAX(version) + 1
            FROM dim_customer
            WHERE customer_id = s.customer_id
        ), 1) AS version,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM staging_customer s
    WHERE EXISTS (
        SELECT 1
        FROM dim_customer d
        WHERE d.customer_id = s.customer_id
          AND d.is_current = FALSE
          AND d.expiration_date = CURRENT_DATE - INTERVAL '1 day'
    );

    -- ========================================================================
    -- STEP 3: Handle Type 1 Attributes (like loyalty points - just update)
    -- ========================================================================
    UPDATE dim_customer d
    SET
        loyalty_points = s.loyalty_points,
        loyalty_tier = s.loyalty_tier,
        loyalty_member = s.loyalty_member,
        updated_at = CURRENT_TIMESTAMP
    FROM staging_customer s
    WHERE d.customer_id = s.customer_id
      AND d.is_current = TRUE
      AND (
          d.loyalty_points != s.loyalty_points OR
          COALESCE(d.loyalty_tier, '') != COALESCE(s.loyalty_tier, '') OR
          COALESCE(d.loyalty_member, FALSE) != COALESCE(s.loyalty_member, FALSE)
      );

    -- Calculate unchanged customers
    v_unchanged_count := v_total_count - v_new_count - v_updated_count;

    -- Return statistics
    RETURN QUERY
    SELECT
        v_new_count AS new_customers,
        v_updated_count AS updated_customers,
        v_unchanged_count AS unchanged_customers,
        v_total_count AS total_processed;

END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Example Usage
-- ============================================================================

COMMENT ON FUNCTION load_dim_customer_scd2() IS
'Loads customer dimension with SCD Type 2 logic.
Handles new customers, tracks historical changes, and updates Type 1 attributes.';

/*
-- To execute:
SELECT * FROM load_dim_customer_scd2();

-- Expected output:
 new_customers | updated_customers | unchanged_customers | total_processed
---------------|-------------------|---------------------|----------------
           150 |                45 |                 805 |            1000
*/

-- ============================================================================
-- Helper Function: Get Customer History
-- ============================================================================

CREATE OR REPLACE FUNCTION get_customer_history(p_customer_id VARCHAR)
RETURNS TABLE (
    customer_key INT,
    customer_name VARCHAR,
    email VARCHAR,
    address VARCHAR,
    customer_segment VARCHAR,
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,
    version INT,
    days_active INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.customer_key,
        d.full_name,
        d.email,
        CONCAT(d.address_line1, ', ', d.city, ', ', d.state, ' ', d.zip_code) AS address,
        d.customer_segment,
        d.effective_date,
        d.expiration_date,
        d.is_current,
        d.version,
        CASE
            WHEN d.is_current THEN CURRENT_DATE - d.effective_date
            ELSE d.expiration_date - d.effective_date
        END AS days_active
    FROM dim_customer d
    WHERE d.customer_id = p_customer_id
    ORDER BY d.version DESC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_customer_history(VARCHAR) IS
'Returns the complete history of a customer including all versions';

/*
-- Example usage:
SELECT * FROM get_customer_history('CUST001');
*/

-- ============================================================================
-- Helper Function: Get Customer as of Date
-- ============================================================================

CREATE OR REPLACE FUNCTION get_customer_as_of_date(
    p_customer_id VARCHAR,
    p_as_of_date DATE
)
RETURNS TABLE (
    customer_key INT,
    customer_name VARCHAR,
    email VARCHAR,
    customer_segment VARCHAR,
    version INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.customer_key,
        d.full_name,
        d.email,
        d.customer_segment,
        d.version
    FROM dim_customer d
    WHERE d.customer_id = p_customer_id
      AND d.effective_date <= p_as_of_date
      AND (d.expiration_date IS NULL OR d.expiration_date >= p_as_of_date)
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_customer_as_of_date(VARCHAR, DATE) IS
'Returns the customer record as it was on a specific date (time travel query)';

/*
-- Example usage:
-- Get customer details as they were on January 1, 2024
SELECT * FROM get_customer_as_of_date('CUST001', '2024-01-01');
*/
