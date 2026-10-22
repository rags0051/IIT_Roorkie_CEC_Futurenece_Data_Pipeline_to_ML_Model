from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'include'))
sys.path.insert(0, '/usr/local/airflow/include')

from nammamart_lib import (  # noqa: E402
    DATA_DIR,
    QUARANTINE_DIR,
    WAREHOUSE_DB,
    append_dq_result,
    append_pipeline_run,
    dedupe_and_standardize,
    validate_dataframe,
    write_quarantine,
)

default_args = {
    'owner': 'rags',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=20),
}


def _run_id():
    return pendulum.now('Asia/Kolkata').strftime('run_%Y%m%d_%H%M%S')


@dag(
    dag_id='nammamart_etl_rags',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata'),
    catchup=False,
    default_args=default_args,
    tags=['etl', 'nammamart', 'rags'],
)
def nammamart_etl_rags():

    @task
    def extract_orders():
        df = pd.read_csv(DATA_DIR / 'orders.csv')
        return {'table': 'orders', 'rows': len(df), 'data': df.to_json(orient='records')}

    @task
    def extract_payments():
        df = pd.read_csv(DATA_DIR / 'payments.csv')
        return {'table': 'payments', 'rows': len(df), 'data': df.to_json(orient='records')}

    @task
    def extract_customers():
        df = pd.read_csv(DATA_DIR / 'customers.csv')
        return {'table': 'customers', 'rows': len(df), 'data': df.to_json(orient='records')}

    @task
    def extract_products():
        df = pd.read_csv(DATA_DIR / 'products.csv')
        return {'table': 'products', 'rows': len(df), 'data': df.to_json(orient='records')}

    @task
    def extract_stores():
        df = pd.read_csv(DATA_DIR / 'stores.csv')
        return {'table': 'stores', 'rows': len(df), 'data': df.to_json(orient='records')}

    @task
    def validate_orders(data):
        run_id = _run_id()
        df = pd.read_json(data['data'])
        passed, failed, checks = validate_dataframe(df, 'orders')
        con = duckdb.connect(str(WAREHOUSE_DB))
        for check_name, dimension, severity, rows_checked, rows_failed in checks:
            append_dq_result(con, 'nammamart_etl_rags', run_id, check_name, dimension, severity, rows_checked, rows_failed, 'PASSED' if len(failed) == 0 else 'FAILED')
        con.close()
        if not failed.empty:
            path = write_quarantine(failed, 'orders', run_id)
        else:
            path = ''
        return {'passed': passed.to_json(orient='records'), 'failed': failed.to_json(orient='records') if not failed.empty else '[]', 'run_id': run_id, 'rows': len(df), 'quarantined': len(failed), 'quarantine_path': path}

    @task
    def validate_payments(data):
        run_id = _run_id()
        df = pd.read_json(data['data'])
        passed, failed, checks = validate_dataframe(df, 'payments')
        con = duckdb.connect(str(WAREHOUSE_DB))
        for check_name, dimension, severity, rows_checked, rows_failed in checks:
            append_dq_result(con, 'nammamart_etl_rags', run_id, check_name, dimension, severity, rows_checked, rows_failed, 'PASSED' if len(failed) == 0 else 'FAILED')
        con.close()
        if not failed.empty:
            path = write_quarantine(failed, 'payments', run_id)
        else:
            path = ''
        return {'passed': passed.to_json(orient='records'), 'failed': failed.to_json(orient='records') if not failed.empty else '[]', 'run_id': run_id, 'rows': len(df), 'quarantined': len(failed), 'quarantine_path': path}

    @task
    def validate_customers(data):
        run_id = _run_id()
        df = pd.read_json(data['data'])
        passed, failed, checks = validate_dataframe(df, 'customers')
        con = duckdb.connect(str(WAREHOUSE_DB))
        for check_name, dimension, severity, rows_checked, rows_failed in checks:
            append_dq_result(con, 'nammamart_etl_rags', run_id, check_name, dimension, severity, rows_checked, rows_failed, 'PASSED' if len(failed) == 0 else 'FAILED')
        con.close()
        if not failed.empty:
            path = write_quarantine(failed, 'customers', run_id)
        else:
            path = ''
        return {'passed': passed.to_json(orient='records'), 'failed': failed.to_json(orient='records') if not failed.empty else '[]', 'run_id': run_id, 'rows': len(df), 'quarantined': len(failed), 'quarantine_path': path}

    @task
    def validate_products(data):
        run_id = _run_id()
        df = pd.read_json(data['data'])
        passed, failed, checks = validate_dataframe(df, 'products')
        con = duckdb.connect(str(WAREHOUSE_DB))
        for check_name, dimension, severity, rows_checked, rows_failed in checks:
            append_dq_result(con, 'nammamart_etl_rags', run_id, check_name, dimension, severity, rows_checked, rows_failed, 'PASSED' if len(failed) == 0 else 'FAILED')
        con.close()
        if not failed.empty:
            path = write_quarantine(failed, 'products', run_id)
        else:
            path = ''
        return {'passed': passed.to_json(orient='records'), 'failed': failed.to_json(orient='records') if not failed.empty else '[]', 'run_id': run_id, 'rows': len(df), 'quarantined': len(failed), 'quarantine_path': path}

    @task
    def validate_stores(data):
        run_id = _run_id()
        df = pd.read_json(data['data'])
        passed, failed, checks = validate_dataframe(df, 'stores')
        con = duckdb.connect(str(WAREHOUSE_DB))
        for check_name, dimension, severity, rows_checked, rows_failed in checks:
            append_dq_result(con, 'nammamart_etl_rags', run_id, check_name, dimension, severity, rows_checked, rows_failed, 'PASSED' if len(failed) == 0 else 'FAILED')
        con.close()
        if not failed.empty:
            path = write_quarantine(failed, 'stores', run_id)
        else:
            path = ''
        return {'passed': passed.to_json(orient='records'), 'failed': failed.to_json(orient='records') if not failed.empty else '[]', 'run_id': run_id, 'rows': len(df), 'quarantined': len(failed), 'quarantine_path': path}

    @task
    def build_star_schema(orders, payments, customers, products, stores):
        con = duckdb.connect(str(WAREHOUSE_DB))
        orders_df = pd.read_json(orders['passed'])
        payments_df = pd.read_json(payments['passed'])
        customers_df = dedupe_and_standardize(pd.read_json(customers['passed']), 'customers')
        products_df = dedupe_and_standardize(pd.read_json(products['passed']), 'products')
        stores_df = dedupe_and_standardize(pd.read_json(stores['passed']), 'stores')
        orders_df = dedupe_and_standardize(orders_df, 'orders')
        payments_df = dedupe_and_standardize(payments_df, 'payments')

        con.execute('CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT * FROM customers_df WHERE 1=0')
        con.execute('CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT * FROM products_df WHERE 1=0')
        con.execute('CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT * FROM stores_df WHERE 1=0')
        con.execute('CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM orders_df WHERE 1=0')

        con.register('customers_df', customers_df)
        con.register('products_df', products_df)
        con.register('stores_df', stores_df)
        con.register('orders_df', orders_df)
        con.execute('DELETE FROM etl_dim_customer')
        con.execute('INSERT INTO etl_dim_customer SELECT * FROM customers_df')
        con.execute('DELETE FROM etl_dim_product')
        con.execute('INSERT INTO etl_dim_product SELECT * FROM products_df')
        con.execute('DELETE FROM etl_dim_store')
        con.execute('INSERT INTO etl_dim_store SELECT * FROM stores_df')
        con.execute('DELETE FROM etl_fact_orders')

        merged = orders_df.merge(customers_df[['customer_id', 'email', 'phone', 'loyalty_tier']], on='customer_id', how='left')
        merged = merged.merge(products_df[['product_id', 'category', 'cost_price']], on='product_id', how='left')
        merged = merged.merge(stores_df[['store_id', 'zone', 'area']], on='store_id', how='left')
        merged['order_date'] = pd.to_datetime(merged['order_ts'], errors='coerce').dt.strftime('%Y-%m-%d')
        merged['net_revenue'] = (pd.to_numeric(merged['quantity'], errors='coerce') * pd.to_numeric(merged['unit_price'], errors='coerce') * (1 - pd.to_numeric(merged['discount_pct'], errors='coerce') / 100)).round(2)
        merged['gross_margin'] = (pd.to_numeric(merged['net_revenue'], errors='coerce') - pd.to_numeric(merged['quantity'], errors='coerce') * pd.to_numeric(merged['cost_price'], errors='coerce')).round(2)
        merged['within_15_min'] = (pd.to_numeric(merged['delivery_minutes'], errors='coerce') <= 15).astype(int)
        merged['email'] = merged['email'].apply(lambda x: x if not x else x)
        merged['phone'] = merged['phone'].apply(lambda x: x if not x else x)

        con.register('fact_df', merged)
        con.execute('INSERT INTO etl_fact_orders SELECT * FROM fact_df')

        payment_mismatches = orders_df.merge(payments_df[['order_id', 'amount_paid', 'payment_status']], on='order_id', how='left')
        payment_mismatches = payment_mismatches.loc[~payment_mismatches['order_id'].isna()]
        payment_mismatches['mismatch_reason'] = payment_mismatches.apply(
            lambda row: 'missing_payment' if pd.isna(row['amount_paid']) else ('amount_mismatch' if abs(float(row['amount_paid']) - float(row['order_amount'])) > 0.01 else 'matched'),
            axis=1,
        )
        payment_mismatches = payment_mismatches[payment_mismatches['mismatch_reason'] != 'matched']
        if not payment_mismatches.empty:
            con.register('recon_df', payment_mismatches[['order_id', 'order_amount', 'amount_paid', 'mismatch_reason']])
            con.execute('CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (order_id VARCHAR, order_amount DOUBLE, amount_paid DOUBLE, mismatch_reason VARCHAR)')
            con.execute('DELETE FROM etl_payment_reconciliation')
            con.execute('INSERT INTO etl_payment_reconciliation SELECT * FROM recon_df')
        else:
            con.execute('CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (order_id VARCHAR, order_amount DOUBLE, amount_paid DOUBLE, mismatch_reason VARCHAR)')
            con.execute('DELETE FROM etl_payment_reconciliation')

        return {'rows_loaded': len(merged), 'reconciled_rows': len(payment_mismatches)}

    @task
    def build_reports():
        con = duckdb.connect(str(WAREHOUSE_DB))
        con.execute('CREATE TABLE IF NOT EXISTS etl_daily_zone_revenue AS SELECT * FROM etl_fact_orders WHERE 1=0')
        con.execute('CREATE TABLE IF NOT EXISTS etl_category_margin AS SELECT * FROM etl_fact_orders WHERE 1=0')
        con.execute('''
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, store_id, zone, SUM(net_revenue) AS net_revenue
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY order_date, store_id, zone
        ''')
        con.execute('''
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT category, SUM(net_revenue) AS revenue, SUM(gross_margin) AS gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY category
        ''')
        print(con.execute('SELECT * FROM etl_daily_zone_revenue ORDER BY order_date LIMIT 10').fetchall())
        print(con.execute('SELECT * FROM etl_category_margin ORDER BY revenue DESC LIMIT 10').fetchall())
        return True

    @task(trigger_rule='all_done')
    def run_audit(**context):
        ti = context['ti']
        rows_extracted = 0
        rows_quarantined = 0
        for task_id in ['extract_orders', 'extract_payments', 'extract_customers', 'extract_products', 'extract_stores']:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                rows_extracted += int(result['rows'])
        for task_id in ['validate_orders', 'validate_payments', 'validate_customers', 'validate_products', 'validate_stores']:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                rows_quarantined += int(result['quarantined'])
        con = duckdb.connect(str(WAREHOUSE_DB))
        append_pipeline_run(con, 'nammamart_etl_rags', _run_id(), pendulum.now('Asia/Kolkata'), pendulum.now('Asia/Kolkata'), rows_extracted, 0, rows_quarantined, 'SUCCESS')
        con.close()
        return {'run_id': _run_id(), 'rows_extracted': rows_extracted, 'rows_quarantined': rows_quarantined}

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()

    order_validation = validate_orders(orders)
    payment_validation = validate_payments(payments)
    customer_validation = validate_customers(customers)
    product_validation = validate_products(products)
    store_validation = validate_stores(stores)

    star_schema = build_star_schema(order_validation, payment_validation, customer_validation, product_validation, store_validation)
    reports = build_reports()
    audit = run_audit()
    return [star_schema, reports, audit]


nammamart_etl_rags = nammamart_etl_rags()
