CREATE OR REPLACE TABLE stg_orders AS
WITH base AS (
    SELECT
        CAST(trim(order_id) AS VARCHAR) AS order_id,
        CAST(order_ts AS TIMESTAMP) AS order_ts,
        CAST(trim(customer_id) AS VARCHAR) AS customer_id,
        CAST(trim(store_id) AS VARCHAR) AS store_id,
        CAST(trim(product_id) AS VARCHAR) AS product_id,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        CAST(discount_pct AS DOUBLE) AS discount_pct,
        CAST(order_amount AS DOUBLE) AS order_amount,
        CASE
            WHEN lower(trim(payment_mode)) = 'upi' THEN 'UPI'
            WHEN lower(trim(payment_mode)) = 'card' THEN 'Card'
            WHEN lower(trim(payment_mode)) = 'cash on delivery' THEN 'Cash on Delivery'
            WHEN lower(trim(payment_mode)) = 'cod' THEN 'Cash on Delivery'
            WHEN lower(trim(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE upper(trim(payment_mode))
        END AS payment_mode,
        CASE WHEN upper(trim(order_status)) IN ('DELIVERED','CANCELLED','RETURNED') THEN upper(trim(order_status)) ELSE upper(trim(order_status)) END AS order_status,
        CAST(trim(channel) AS VARCHAR) AS channel,
        CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id
    FROM raw_orders
)
SELECT *,
       CASE
           WHEN order_id IS NULL OR customer_id IS NULL OR store_id IS NULL OR product_id IS NULL OR quantity <= 0 OR unit_price <= 0 OR discount_pct < 0 OR discount_pct > 100 OR order_amount IS NULL THEN 'FAIL'
           WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'FAIL'
           WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'FAIL'
           WHEN order_status = 'DELIVERED' AND (delivery_minutes IS NULL OR delivery_minutes < 8 OR delivery_minutes > 120) THEN 'FAIL'
           ELSE 'PASS'
       END AS dq_status,
       CASE
           WHEN order_id IS NULL OR customer_id IS NULL OR store_id IS NULL OR product_id IS NULL OR quantity <= 0 OR unit_price <= 0 OR discount_pct < 0 OR discount_pct > 100 OR order_amount IS NULL THEN 'missing_required_fields'
           WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'invalid_payment_mode'
           WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'invalid_order_status'
           WHEN order_status = 'DELIVERED' AND (delivery_minutes IS NULL OR delivery_minutes < 8 OR delivery_minutes > 120) THEN 'delivery_minutes_out_of_range'
           ELSE NULL
       END AS failure_reason
FROM base;

CREATE OR REPLACE TABLE stg_payments AS
SELECT
    CAST(trim(payment_id) AS VARCHAR) AS payment_id,
    CAST(trim(order_id) AS VARCHAR) AS order_id,
    CAST(payment_ts AS TIMESTAMP) AS payment_ts,
    CASE
        WHEN lower(trim(payment_mode)) = 'upi' THEN 'UPI'
        WHEN lower(trim(payment_mode)) = 'card' THEN 'Card'
        WHEN lower(trim(payment_mode)) = 'cash on delivery' THEN 'Cash on Delivery'
        WHEN lower(trim(payment_mode)) = 'cod' THEN 'Cash on Delivery'
        WHEN lower(trim(payment_mode)) = 'wallet' THEN 'Wallet'
        ELSE upper(trim(payment_mode))
    END AS payment_mode,
    CAST(amount_paid AS DOUBLE) AS amount_paid,
    CAST(trim(payment_status) AS VARCHAR) AS payment_status,
    CAST(trim(gateway_ref) AS VARCHAR) AS gateway_ref,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN payment_id IS NULL OR order_id IS NULL OR payment_ts IS NULL OR amount_paid IS NULL THEN 'FAIL'
        WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN payment_id IS NULL OR order_id IS NULL OR payment_ts IS NULL OR amount_paid IS NULL THEN 'missing_required_fields'
        WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'invalid_payment_mode'
        ELSE NULL
    END AS failure_reason
FROM raw_payments;

CREATE OR REPLACE TABLE stg_customers AS
SELECT
    CAST(trim(customer_id) AS VARCHAR) AS customer_id,
    CAST(trim(full_name) AS VARCHAR) AS full_name,
    CASE WHEN email IS NULL THEN NULL ELSE lower(trim(email)) END AS email,
    CAST(regexp_replace(trim(phone), '[^0-9]', '') AS VARCHAR) AS phone,
    CAST(trim(city) AS VARCHAR) AS city,
    CAST(trim(area) AS VARCHAR) AS area,
    CAST(trim(pincode) AS VARCHAR) AS pincode,
    CAST(signup_date AS DATE) AS signup_date,
    CASE WHEN upper(trim(loyalty_tier)) IN ('BRONZE','SILVER','GOLD','PLATINUM') THEN upper(trim(loyalty_tier)) ELSE upper(trim(loyalty_tier)) END AS loyalty_tier,
    CASE WHEN upper(trim(is_active)) IN ('Y','N') THEN upper(trim(is_active)) ELSE upper(trim(is_active)) END AS is_active,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN customer_id IS NULL OR email IS NULL OR phone IS NULL OR loyalty_tier IS NULL THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN customer_id IS NULL OR email IS NULL OR phone IS NULL OR loyalty_tier IS NULL THEN 'missing_required_fields'
        ELSE NULL
    END AS failure_reason
FROM raw_customers;

CREATE OR REPLACE TABLE stg_products AS
SELECT
    CAST(trim(product_id) AS VARCHAR) AS product_id,
    CAST(trim(product_name) AS VARCHAR) AS product_name,
    CAST(trim(category) AS VARCHAR) AS category,
    CAST(trim(sub_category) AS VARCHAR) AS sub_category,
    CAST(trim(brand) AS VARCHAR) AS brand,
    CAST(mrp AS DOUBLE) AS mrp,
    CAST(cost_price AS DOUBLE) AS cost_price,
    CAST(trim(unit) AS VARCHAR) AS unit,
    CAST(is_perishable AS BOOLEAN) AS is_perishable,
    CAST(launch_date AS DATE) AS launch_date,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN product_id IS NULL OR category IS NULL OR mrp IS NULL OR cost_price IS NULL THEN 'FAIL'
        WHEN mrp <= 0 OR cost_price <= 0 OR cost_price > mrp THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN product_id IS NULL OR category IS NULL OR mrp IS NULL OR cost_price IS NULL THEN 'missing_required_fields'
        WHEN mrp <= 0 OR cost_price <= 0 OR cost_price > mrp THEN 'price_validation_fail'
        ELSE NULL
    END AS failure_reason
FROM raw_products;

CREATE OR REPLACE TABLE stg_stores AS
SELECT
    CAST(trim(store_id) AS VARCHAR) AS store_id,
    CAST(trim(store_name) AS VARCHAR) AS store_name,
    CAST(trim(zone) AS VARCHAR) AS zone,
    CAST(trim(area) AS VARCHAR) AS area,
    CAST(trim(pincode) AS VARCHAR) AS pincode,
    CAST(open_date AS DATE) AS open_date,
    CAST(trim(manager_name) AS VARCHAR) AS manager_name,
    CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN store_id IS NULL OR zone IS NULL OR capacity_orders_per_day IS NULL THEN 'FAIL'
        WHEN capacity_orders_per_day <= 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN store_id IS NULL OR zone IS NULL OR capacity_orders_per_day IS NULL THEN 'missing_required_fields'
        WHEN capacity_orders_per_day <= 0 THEN 'capacity_invalid'
        ELSE NULL
    END AS failure_reason
FROM raw_stores;
