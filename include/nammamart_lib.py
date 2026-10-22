from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import duckdb
import pandas as pd

DATA_DIR = Path('/usr/local/airflow/include/data/nammamart')
WAREHOUSE_DB = Path('/usr/local/airflow/include/warehouse_rags.duckdb')
QUARANTINE_DIR = Path('/usr/local/airflow/include/quarantine/rags')

VALID_ORDER_STATUS = {'DELIVERED', 'CANCELLED', 'RETURNED'}
VALID_PAYMENT_MODE = {'UPI', 'Card', 'Cash on Delivery', 'Wallet'}
VALID_LOYALTY_TIERS = {'BRONZE', 'SILVER', 'GOLD', 'PLATINUM'}
VALID_ZONES = {'North', 'South', 'East', 'West'}
VALID_CATEGORIES = {'Fruits & Vegetables', 'Dairy & Bakery', 'Household', 'Personal Care', 'Snacks & Beverages', 'Staples'}


def normalize_text(value):
    if pd.isna(value):
        return ''
    return str(value).strip()


def normalise_payment_mode(value):
    text = normalize_text(value).lower()
    mapping = {
        'upi': 'UPI',
        'upi ': 'UPI',
        'card': 'Card',
        'cash on delivery': 'Cash on Delivery',
        'cashh': 'Cash on Delivery',
        'cod': 'Cash on Delivery',
        'wallet': 'Wallet',
    }
    return mapping.get(text, normalize_text(value).title() if normalize_text(value) else '')


def normalise_status(value):
    status = normalize_text(value).upper()
    if status in {'DELIVERED', 'CANCELLED', 'RETURNED'}:
        return status
    return status


def mask_email(value):
    email = normalize_text(value)
    if not email or '@' not in email:
        return email
    local, domain = email.split('@', 1)
    prefix = local[:3]
    masked_local = f'{prefix}***' if len(local) > 3 else local
    return f'{masked_local}@{domain}'


def mask_phone(value):
    digits = re.sub(r'\D', '', normalize_text(value))
    if len(digits) >= 10:
        return f'{digits[:2]}******{digits[-2:]}'
    return digits


def dedupe_and_standardize(df, table_name):
    df = df.copy()
    for column in df.columns:
        if df[column].dtype == 'object' or pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].map(lambda x: normalize_text(x))
    if table_name == 'customers':
        df['email'] = df['email'].map(mask_email)
        df['phone'] = df['phone'].map(mask_phone)
        df['loyalty_tier'] = df['loyalty_tier'].map(lambda x: normalize_text(x).upper())
        df['is_active'] = df['is_active'].map(lambda x: normalize_text(x).upper())
        return df.drop_duplicates(subset=['customer_id'], keep='last')
    if table_name == 'products':
        return df.drop_duplicates(subset=['product_id'], keep='last')
    if table_name == 'stores':
        return df.drop_duplicates(subset=['store_id'], keep='last')
    if table_name == 'orders':
        df['payment_mode'] = df['payment_mode'].map(normalise_payment_mode)
        df['order_status'] = df['order_status'].map(normalise_status)
        return df.drop_duplicates(subset=['order_id'], keep='last')
    if table_name == 'payments':
        df['payment_mode'] = df['payment_mode'].map(normalise_payment_mode)
        df['payment_status'] = df['payment_status'].map(lambda x: normalize_text(x).upper())
        return df.drop_duplicates(subset=['payment_id'], keep='last')
    return df


def write_quarantine(df: pd.DataFrame, file_name: str, run_id: str):
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = QUARANTINE_DIR / f'{file_name}_{run_id}_failed.csv'
    df.to_csv(out_path, index=False)
    return str(out_path)


def append_dq_result(con, dag_id, run_id, check_name, dimension, severity, rows_checked, rows_failed, status):
    con.execute(
        '''
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR,
            dag_id VARCHAR,
            check_name VARCHAR,
            dimension VARCHAR,
            severity VARCHAR,
            rows_checked BIGINT,
            rows_failed BIGINT,
            status VARCHAR,
            checked_at TIMESTAMP
        )
        '''
    )
    con.execute(
        'INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)',
        [run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status],
    )


def append_pipeline_run(con, dag_id, run_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status):
    con.execute(
        '''
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            dag_id VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            rows_extracted BIGINT,
            rows_loaded BIGINT,
            rows_quarantined BIGINT,
            status VARCHAR
        )
        '''
    )
    con.execute(
        'INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status],
    )


def validate_dataframe(df: pd.DataFrame, table_name: str) -> Tuple[pd.DataFrame, pd.DataFrame, List[Tuple[str, str, str, int, int]]]:
    failed_rows = []
    for idx, row in df.iterrows():
        reasons = []
        if table_name == 'orders':
            required = ['order_id', 'order_ts', 'customer_id', 'store_id', 'product_id', 'quantity', 'unit_price', 'discount_pct', 'order_amount', 'payment_mode', 'order_status', 'channel', 'delivery_minutes']
            for col in required:
                if pd.isna(row.get(col)) or normalize_text(row.get(col)) == '':
                    reasons.append(f'missing:{col}')
            qty = pd.to_numeric(row.get('quantity'), errors='coerce')
            unit_price = pd.to_numeric(row.get('unit_price'), errors='coerce')
            discount = pd.to_numeric(row.get('discount_pct'), errors='coerce')
            order_amount = pd.to_numeric(row.get('order_amount'), errors='coerce')
            delivery_mins = pd.to_numeric(row.get('delivery_minutes'), errors='coerce')
            if pd.isna(qty) or qty <= 0:
                reasons.append('quantity_invalid')
            if pd.isna(unit_price) or unit_price <= 0:
                reasons.append('price_invalid')
            if pd.isna(discount) or discount < 0 or discount > 100:
                reasons.append('discount_invalid')
            if not pd.isna(qty) and not pd.isna(unit_price) and not pd.isna(discount):
                expected = round(float(qty) * float(unit_price) * (1 - float(discount) / 100), 2)
                if not pd.isna(order_amount) and abs(float(order_amount) - expected) > 0.01:
                    reasons.append('order_amount_mismatch')
            mode = normalise_payment_mode(row.get('payment_mode'))
            status = normalise_status(row.get('order_status'))
            if mode not in VALID_PAYMENT_MODE:
                reasons.append('payment_mode_invalid')
            if status not in VALID_ORDER_STATUS:
                reasons.append('order_status_invalid')
            if str(status) == 'DELIVERED' and (pd.isna(delivery_mins) or delivery_mins < 8 or delivery_mins > 120):
                reasons.append('delivery_minutes_invalid')
            if pd.to_datetime(row.get('order_ts'), errors='coerce') is pd.NaT:
                reasons.append('timestamp_invalid')
        elif table_name == 'payments':
            required = ['payment_id', 'order_id', 'payment_ts', 'payment_mode', 'amount_paid', 'payment_status']
            for col in required:
                if pd.isna(row.get(col)) or normalize_text(row.get(col)) == '':
                    reasons.append(f'missing:{col}')
            if pd.to_numeric(row.get('amount_paid'), errors='coerce') is not None and pd.to_numeric(row.get('amount_paid'), errors='coerce') < 0:
                reasons.append('negative_amount')
            if normalise_payment_mode(row.get('payment_mode')) not in VALID_PAYMENT_MODE:
                reasons.append('payment_mode_invalid')
        elif table_name == 'customers':
            required = ['customer_id', 'full_name', 'email', 'phone', 'city', 'area', 'pincode', 'signup_date', 'loyalty_tier', 'is_active']
            for col in required:
                if pd.isna(row.get(col)) or normalize_text(row.get(col)) == '':
                    reasons.append(f'missing:{col}')
            if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', normalize_text(row.get('email'))):
                reasons.append('email_invalid')
            if not re.match(r'^\d{10}$', re.sub(r'\D', '', normalize_text(row.get('phone')))):
                reasons.append('phone_invalid')
        elif table_name == 'products':
            required = ['product_id', 'product_name', 'category', 'sub_category', 'brand', 'mrp', 'cost_price', 'unit', 'is_perishable', 'launch_date']
            for col in required:
                if pd.isna(row.get(col)) or normalize_text(row.get(col)) == '':
                    reasons.append(f'missing:{col}')
            mrp = pd.to_numeric(row.get('mrp'), errors='coerce')
            cost = pd.to_numeric(row.get('cost_price'), errors='coerce')
            if pd.isna(mrp) or mrp <= 0:
                reasons.append('mrp_invalid')
            if pd.isna(cost) or cost <= 0:
                reasons.append('cost_invalid')
            if pd.notna(mrp) and pd.notna(cost) and cost > mrp:
                reasons.append('cost_gt_mrp')
            if normalize_text(row.get('category')) not in VALID_CATEGORIES:
                reasons.append('category_invalid')
        elif table_name == 'stores':
            required = ['store_id', 'store_name', 'zone', 'area', 'pincode', 'open_date', 'manager_name', 'capacity_orders_per_day']
            for col in required:
                if pd.isna(row.get(col)) or normalize_text(row.get(col)) == '':
                    reasons.append(f'missing:{col}')
            if normalize_text(row.get('zone')) not in VALID_ZONES:
                reasons.append('zone_invalid')
            if pd.to_numeric(row.get('capacity_orders_per_day'), errors='coerce') is not None and pd.to_numeric(row.get('capacity_orders_per_day'), errors='coerce') <= 0:
                reasons.append('capacity_invalid')
        if reasons:
            row = row.to_dict()
            row['failure_reason'] = '; '.join(reasons)
            failed_rows.append(row)
    failed_df = pd.DataFrame(failed_rows)
    passed = df.loc[~df.index.isin(failed_df.index)] if not failed_df.empty else df.iloc[0:0]
    return passed, failed_df, [
        (f'{table_name}_required_fields', 'Completeness', 'Critical', len(df), len(failed_df) if not failed_df.empty else 0),
        (f'{table_name}_validity', 'Validity', 'Critical', len(df), len(failed_df) if not failed_df.empty else 0),
    ]


def estimate_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_ts'], errors='coerce').dt.date
    df['net_revenue'] = (pd.to_numeric(df['quantity'], errors='coerce') * pd.to_numeric(df['unit_price'], errors='coerce') * (1 - pd.to_numeric(df['discount_pct'], errors='coerce') / 100)).round(2)
    df['gross_margin'] = (pd.to_numeric(df['net_revenue'], errors='coerce') - pd.to_numeric(df['quantity'], errors='coerce') * pd.to_numeric(df['cost_price'], errors='coerce')).round(2)
    df['within_15_min'] = (pd.to_numeric(df['delivery_minutes'], errors='coerce') <= 15).astype(int)
    return df


def query_warehouse(db_path: str = '/usr/local/airflow/include/warehouse_rags.duckdb'):
    con = duckdb.connect(db_path)
    tables = con.execute("SHOW TABLES").fetchall()
    return {name for (name,) in tables}


def create_warehouse_db(path: str = '/usr/local/airflow/include/warehouse_rags.duckdb'):
    con = duckdb.connect(path)
    return con
