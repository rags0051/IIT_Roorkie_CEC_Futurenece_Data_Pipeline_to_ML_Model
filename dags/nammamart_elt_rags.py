from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'include'))
sys.path.insert(0, '/usr/local/airflow/include')

from nammamart_lib import WAREHOUSE_DB, append_pipeline_run  # noqa: E402

BASE_DIR = Path('/usr/local/airflow/include/sql')


default_args = {
    'owner': 'rags',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=20),
}


def _run_id():
    return pendulum.now('Asia/Kolkata').strftime('run_%Y%m%d_%H%M%S')


def _execute_sql(path: str, run_id: str):
    conn = duckdb.connect(str(WAREHOUSE_DB))
    sql = Path(path).read_text()
    sql = sql.replace(':run_id', f"'{run_id}'")
    conn.execute(sql)
    conn.close()
    return True


@dag(
    dag_id='nammamart_elt_rags',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata'),
    catchup=False,
    default_args=default_args,
    tags=['elt', 'nammamart', 'rags'],
)
def nammamart_elt_rags():

    @task
    def load_raw_data():
        run_id = _run_id()
        _execute_sql(str(BASE_DIR / 'elt_raw_load.sql'), run_id)
        return {'run_id': run_id, 'status': 'raw_loaded'}

    @task
    def stage_data():
        run_id = _run_id()
        _execute_sql(str(BASE_DIR / 'elt_stage_transform.sql'), run_id)
        return {'run_id': run_id, 'status': 'staging_complete'}

    @task
    def incremental_load():
        run_id = _run_id()
        _execute_sql(str(BASE_DIR / 'elt_incremental_orders.sql'), run_id)
        return {'run_id': run_id, 'status': 'incremental_applied'}

    @task
    def build_marts():
        run_id = _run_id()
        _execute_sql(str(BASE_DIR / 'elt_mart_build.sql'), run_id)
        con = duckdb.connect(str(WAREHOUSE_DB))
        print(con.execute('SELECT * FROM mart_daily_store_kpis LIMIT 5').fetchall())
        print(con.execute('SELECT * FROM mart_payment_reconciliation LIMIT 5').fetchall())
        con.close()
        return {'run_id': run_id, 'status': 'mart_ready'}

    @task(trigger_rule='all_done')
    def run_audit(**context):
        ti = context['ti']
        conn = duckdb.connect(str(WAREHOUSE_DB))
        run_id = _run_id()
        rows_extracted = 0
        _ = ti.xcom_pull(task_ids='load_raw_data')
        for task_name in ['load_raw_data', 'stage_data', 'incremental_load', 'build_marts']:
            result = ti.xcom_pull(task_ids=task_name)
            if result:
                rows_extracted += 1
        append_pipeline_run(conn, 'nammamart_elt_rags', run_id, pendulum.now('Asia/Kolkata'), pendulum.now('Asia/Kolkata'), rows_extracted, 0, 0, 'SUCCESS')
        conn.close()
        return {'run_id': run_id, 'rows_extracted': rows_extracted}

    raw = load_raw_data()
    staged = stage_data()
    incremental = incremental_load()
    marts = build_marts()
    audit = run_audit()
    return [raw, staged, incremental, marts, audit]


nammamart_elt_rags = nammamart_elt_rags()
