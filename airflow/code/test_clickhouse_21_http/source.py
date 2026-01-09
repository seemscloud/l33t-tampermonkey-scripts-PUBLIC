import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

SQL_QUERY = """
with date as(
  select '2025-02-05'::Date dt
),
a as(
  select distinct date(created_at) dt,
         upper(number) number,
         product_id,
         order_id,
         case when element_id in ('add','plus') then 1 else 0 end as element
  from logs.universal_logs
  where toDate(created_at) = (select dt from date)
    and product_id > 0
    and match(number, '^[A-Za-z0-9]+$')
),
ch as(
  select distinct date(check_dttm) dt,
         date(order_dttm) dt_,
         upper(bonuscard) bonuscard,
         basket_id,
         sku_id,
         base_amt
  from report.v_check
  where date(check_dttm) = (select dt from date)
    and bonuscard != ''
)
select a.dt as date,
       a.number,
       a.product_id,
       max(a.element) as add2cart,
       max(if(ch.sku_id is not null, 1, 0)) as is_order
from a
left join ch c
       on c.bonuscard = a.number
      and toUInt64(c.sku_id) = a.product_id
      and c.dt_ = a.dt
      and c.basket_id = a.order_id
group by a.dt, a.number, a.product_id, a.order_id
limit 3
"""


def run_clickhouse_query(**context):
    conn = BaseHook.get_connection("clickhouse_21")
    logging.info(f"ClickHouse host={conn.host}, port={conn.port}, db={conn.schema}")

    host = conn.host
    port = conn.port or 8123
    user = conn.login
    password = conn.password
    database = conn.schema or "default"

    url = f"http://{host}:{port}/?database={database}"
    logging.info(f"Подключаюсь к {url} ...")

    try:
        response = requests.post(
            url,
            data=SQL_QUERY.encode("utf-8"),
            auth=(user, password),
            headers={"Content-Type": "text/plain"},
            timeout=30,  # <<< чтобы не висло вечно
        )
    except Exception as e:
        logging.error(f"Ошибка при запросе: {e}")
        raise

    logging.info(f"HTTP status = {response.status_code}")
    logging.info(f"Ответ (первые 500 байт):\n{response.text[:500]}")

    if response.status_code != 200:
        raise Exception(f"ClickHouse HTTP error {response.status_code}: {response.text}")

    lines = response.text.strip().splitlines()
    sample = lines[:3]
    logging.info("=== Первые строки ===")
    for row in sample:
        logging.info(row)


with DAG(
    dag_id="test_clickhouse_21_http",
    description="Тестовое подключение к ClickHouse 21 через HTTP",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "clickhouse"],
) as dag:
    test_clickhouse = PythonOperator(
        task_id="test_clickhouse_query",
        python_callable=run_clickhouse_query,
    )
