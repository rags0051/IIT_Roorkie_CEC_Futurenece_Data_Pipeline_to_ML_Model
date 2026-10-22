CREATE OR REPLACE TEMP TABLE tmp_increment_orders AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
       'orders_2026-10-01_increment.csv' AS _source_file,
       :run_id AS _run_id
FROM read_csv_auto('/usr/local/airflow/include/data/nammamart/orders_2026-10-01_increment.csv', all_varchar=true);

MERGE INTO stg_orders AS target
USING (
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
    FROM tmp_increment_orders
) AS src
ON target.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET
    target.order_ts = src.order_ts,
    target.customer_id = src.customer_id,
    target.store_id = src.store_id,
    target.product_id = src.product_id,
    target.quantity = src.quantity,
    target.unit_price = src.unit_price,
    target.discount_pct = src.discount_pct,
    target.order_amount = src.order_amount,
    target.payment_mode = src.payment_mode,
    target.order_status = src.order_status,
    target.channel = src.channel,
    target.delivery_minutes = src.delivery_minutes,
    target._loaded_at = src._loaded_at,
    target._source_file = src._source_file,
    target._run_id = src._run_id
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes, _loaded_at, _source_file, _run_id, dq_status, failure_reason
)
VALUES (
    src.order_id, src.order_ts, src.customer_id, src.store_id, src.product_id, src.quantity, src.unit_price, src.discount_pct,
    src.order_amount, src.payment_mode, src.order_status, src.channel, src.delivery_minutes,
    src._loaded_at, src._source_file, src._run_id,
    CASE WHEN src.payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') OR src.order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'FAIL' ELSE 'PASS' END,
    NULL
);
