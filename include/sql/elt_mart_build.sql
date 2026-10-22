CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.store_id,
    s.zone,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS net_revenue,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
LEFT JOIN stg_products p ON o.product_id = p.product_id
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
GROUP BY 1,2,3;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS revenue,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN (o.order_amount - (o.quantity * p.cost_price)) ELSE 0 END) AS gross_margin
FROM stg_orders o
LEFT JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes END) AS avg_delivery_minutes,
    AVG(CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END) * 100 AS pct_delivered_within_15_min
FROM stg_orders o
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY o.store_id;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT
    o.order_id,
    o.order_amount,
    p.amount_paid,
    CASE
        WHEN p.amount_paid IS NULL THEN 'missing_payment'
        WHEN abs(p.amount_paid - o.order_amount) > 0.01 THEN 'amount_mismatch'
        ELSE 'matched'
    END AS mismatch_reason
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED';

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    SUM(o.order_amount) AS total_spend,
    c.loyalty_tier
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
