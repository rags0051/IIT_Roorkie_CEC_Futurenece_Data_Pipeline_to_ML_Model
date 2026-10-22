# NammaMart ETL/ELT Submission

## Part 0: Data Issues Log

| File | Row key | Column | Problem | Dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| orders.csv | ORD#### | payment_mode | Mixed casing and variants like `upi`, `UPI `, `Card`, `Cashh` | Validity | Standardize to allowed list before load | Critical |
| orders.csv | ORD#### | order_status | Invalid values such as `Pending??` or `delivered` | Validity | Only `DELIVERED`, `CANCELLED`, `RETURNED` accepted | Critical |
| orders.csv | ORD#### | quantity | Negative or zero quantity values | Accuracy | `quantity > 0` for valid orders | Critical |
| orders.csv | ORD#### | order_amount | Formula mismatch vs `quantity * unit_price * (1 - discount_pct/100)` | Accuracy | Recompute and compare within 0.01 tolerance | Critical |
| orders.csv | ORD#### | delivery_minutes | Missing or impossible delivery times | Completeness / Accuracy | DELIVERED orders need 8–120 minutes | Critical |
| payments.csv | PAY#### | payment_mode | Inconsistent payment-mode labels | Validity | Map to canonical values `UPI`, `Card`, `Cash on Delivery`, `Wallet` | Critical |
| payments.csv | PAY#### | amount_paid | Negative or mismatched amounts | Accuracy | `amount_paid >= 0` and equal to order amount for SUCCESS rows | Critical |
| customers.csv | C##### | email | Invalid or missing email addresses | Validity | Email format check; mask before reporting | Warning |
| customers.csv | C##### | phone | 10-digit validation issues; unmasked raw data in source | Validity / Personal data | Validate 10 digits; mask in reporting tables | Warning |
| customers.csv | C##### | customer_id | Duplicate keys present | Uniqueness | Keep one canonical customer row per `customer_id` | Critical |
| products.csv | P#### | category | Missing or invalid category labels | Completeness | Category must be in allowed category set | Critical |
| products.csv | P#### | cost_price | Higher than MRP or invalid price | Accuracy | `cost_price <= mrp` and price > 0 | Critical |
| products.csv | P#### | product_id | Duplicate IDs across catalogue | Uniqueness | Deduplicate to one row per product | Critical |
| stores.csv | S## | zone | Missing or invalid zone value | Consistency | Zone must be one of North/South/East/West | Warning |
| stores.csv | S## | capacity_orders_per_day | Zero or null capacity | Accuracy | Capacity must be positive | Warning |

### Three questions to ask the client before build
1. Should a negative quantity be treated as a return or as a data-entry error? 
2. For returned orders, do we keep the original order in historical revenue and only exclude it from current revenue? 
3. Is a `payment_status = FAILED` row allowed to persist in raw history but never count in finance totals? 

## Part C: Scheduling answers
- Cron expression: `0 9 * * 1-6`
- Airflow UI UTC time: 03:30 UTC for a 9:00 AM IST run.
- If `start_date` stayed in UTC: the DAG would run at the wrong local time, causing the pipeline to hit after the 9:00 AM IST SLA and distort business operations.
- If the laptop or Docker is off at 9 AM: no run occurs until the service restarts; in production, the Airflow scheduler must be continuously running or the pipeline would miss the SLA.

## Part E: Data governance answers
1. Ownership: Orders and payments are owned by Operations and Finance respectively; customers by CRM/Compliance; products by Merchandising; stores by Retail Ops; warehouse tables by Analytics/Data Engineering. A quality-rule change such as the 120-minute delivery cap is approved by Ops, Finance, and Data Governance.
2. Data dictionary: Fact orders = `order_id`, `customer_id`, `store_id`, `product_id`, `order_ts`, `quantity`, `unit_price`, `discount_pct`, `order_amount`, `payment_mode`, `order_status`, `channel`, `delivery_minutes`; dimensions include customers, products, stores. PII is limited to customer email and phone in the raw/customer dimension; masked versions are used in reporting tables.
3. Lineage: In ELT, a KPI such as `mart_daily_store_kpis.net_revenue` can be traced back through `stg_orders` to `raw_orders` via `order_id`, store joins, and `order_amount` logic. ETL can also trace lineage, but ELT is usually easier because raw and staging tables preserve the row-level source mapping and run_id.
4. Personal data: `full_name`, `email`, `phone`, `city`, `area`, `pincode`, `signup_date` are personal data. We mask email and phone before reporting and keep raw data only in controlled access layers. The applicable Indian law is the Digital Personal Data Protection Act, 2023, which expects purpose limitation, security safeguards, and minimization.
5. Access control: Raw exports should be readable only by Data Engineering and Compliance; staging by Analytics and QA; marts by Finance/Operations; quarantine by Data Stewardship and Finance. Raw customers with unmasked phone numbers should be restricted to approved data stewards only.
6. Retention: Raw data and quarantine files should be retained for a defined retention period (for example 1–2 years) and then archived or deleted securely; masked customer data can be kept longer for reporting but should be subject to access review and deletion policies after the retention period.
7. Secrets: credentials such as `SLACK_WEBHOOK_URL` should live in Airflow Variables or secrets backends, never in DAG code or committed to GitHub.
8. Audit: In the ETL pipeline, a rejected order can be recovered from the quarantine CSV and matched to the failing rule. In ELT, the same evidence is stored in raw tables and staging failures using `failure_reason`; finance can verify the row is excluded from revenue totals because the fact table only contains passed rows.
9. Change history: If an order was delivered on 3 September and then returned on 1 October, the September ETL fact table should reflect the state as of 30 September, while the ELT raw and incremental history retains both statuses. Finance generally wants the September revenue based on the valid version at the reporting cutoff, while analytics keeps both raw and corrected historical views.

## Part G: Recommendation to the client
NammaMart should adopt ETL as the primary operational finance layer and retain ELT as an audit/analytics layer. ETL gives stronger data-quality guarantees, keeps invalid transactions out of finance tables, and is easier to explain to CFO and Ops stakeholders because the warehouse contains only trusted rows. ELT is valuable for raw replay, compliance, and exploratory analytics, but for daily executive reporting the safety and determinism of ETL are more important. The main risks are rule drift, complexity in reconciliation, and operational overhead, which can be managed through a small set of approved data-quality rules and strong lineage tracking.

## Pipeline implementation notes
- Created ETL DAG: `dags/nammamart_etl_rags.py`
- Created ELT DAG: `dags/nammamart_elt_rags.py`
- Added helper functions: `include/nammamart_lib.py`
- Added SQL fixtures in `include/sql/`
- Data copied into `include/data/nammamart/`

## Validation
- `python -m py_compile dags/nammamart_etl_rags.py dags/nammamart_elt_rags.py include/nammamart_lib.py`
- `DagBag` import test passed with no import errors.
