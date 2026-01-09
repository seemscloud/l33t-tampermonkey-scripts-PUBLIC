import os
import pathlib
import pandas as pd
import glob
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from gcp_connection.connection_info import GCPConnectionInfo

# Required Python 3.8
from google.analytics.data_v1beta.services.beta_analytics_data.client import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    OrderBy
)

from pytz import timezone
from datetime import datetime, timedelta

BQ_DATASET = 'ga4_gy'
store_directory = 'gy'
# property id ga4 gy
property_id = "428643589"
local_tz = timezone('America/Guyana')
today_date = datetime.now(tz=local_tz).strftime('%Y-%m-%d')
file_name = datetime.now(tz=local_tz).strftime('%Y-%m-%d') + '.csv'
directory_files_path = '/opt/airflow/files'
file_overview_dir_path = directory_files_path + '/ga4/' + store_directory + '/overview'
file_overview_session_source_medium_dir_path = directory_files_path + '/ga4/' + store_directory + '/overview_session_medium'
bq_table_overview = 'overview_medium'
bq_table_overview_session_source_medium = 'overview_session_source_medium'
csv_overview_file_path = file_overview_dir_path + '/' + file_name
csv_overview_session_source_medium_file_path = file_overview_session_source_medium_dir_path + '/' + file_name
last_30_day_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
ga4_start_str = Variable.get('ga4_start_str') if Variable.get('ga4_start_str').lower() != "none" else last_30_day_ago
ga4_finish_str = Variable.get('ga4_finish_str') if Variable.get('ga4_finish_str').lower() != "none" else 'yesterday'
limit = 10000

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/airflow_service_account.json'

GCP_CONN = Variable.get("gcp_conn")
BUCKET = Variable.get("ga4_bucket")
PROJECT = Variable.get("gcp_project_id")

email_on_failure = Variable.get("email_on_failure").rstrip(',').split(',')
default_args = {
    "email": email_on_failure,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "max_retry_delay": timedelta(minutes=15),
}

client = BetaAnalyticsDataClient()


def run_report_overview_medium():
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="bounceRate"),
            Metric(name='totalRevenue'),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="transactions"),
            Metric(name='addToCarts')
        ],
        date_ranges=[DateRange(start_date="2019-01-01", end_date="yesterday")],
        order_bys=[
            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)
        ]
    )
    response = client.run_report(request)
    result = response.rows
    return result


def run_report_overview_session_source_medium(offset=0):
    result = []
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name='sessionSourceMedium')
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="bounceRate"),
            Metric(name='totalRevenue'),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="transactions"),
            Metric(name='addToCarts')
        ],
        date_ranges=[DateRange(start_date=ga4_start_str, end_date=ga4_finish_str)],
        order_bys=[
            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)
        ],
        offset=offset,
        limit=limit
    )
    response = client.run_report(request)
    result += response.rows
    if response.row_count > offset:
        result += run_report_overview_session_source_medium(offset + limit)
    return result


def get_ga4_overview_medium_data_to_local():
    source_data = []
    raw_data = run_report_overview_medium()
    if len(raw_data) < 1:
        return False
    for row in raw_data:
        date_int = row.dimension_values[0].value
        date_str = datetime.strptime(date_int, '%Y%m%d').strftime('%Y-%m-%d')
        dimension_data = {
            'date': date_str,
        }
        key_metric_data = ['sessions', 'bounce_rate', 'total_revenue', 'total_users', 'new_users', 'transactions',
                           'add_to_carts']
        values_metric_data = []
        for metric in row.metric_values:
            if metric.value.isdigit():
                values_metric_data.append(int(metric.value))
            else:
                values_metric_data.append(round(float(metric.value), 2))
        metric_data = dict(zip(key_metric_data, values_metric_data))

        additional_data = {
            'created_at': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
        }
        row_data = {**dimension_data, **metric_data, **additional_data}
        source_data.append(row_data)
    if not os.path.exists(f"{file_overview_dir_path}"):
        os.makedirs(f"{file_overview_dir_path}")
    df = pd.DataFrame(data=source_data)
    df.to_csv(f"{file_overview_dir_path}/{today_date}.csv", index=False)
    return True


def get_ga4_overview_session_source_medium_data_to_local():
    source_data = []
    raw_data = run_report_overview_session_source_medium()
    file_index = 1
    current_record_index = 1
    total_record = len(raw_data)
    if len(raw_data) < 1:
        return False
    for row in raw_data:
        date_int = row.dimension_values[0].value
        date_str = datetime.strptime(date_int, '%Y%m%d').strftime('%Y-%m-%d')
        dimension_data = {
            'date': date_str,
        }
        key_metric_data = ['sessions', 'bounce_rate', 'total_revenue', 'total_users', 'new_users', 'transactions',
                           'add_to_carts']
        values_metric_data = []
        for metric in row.metric_values:
            if metric.value.isdigit():
                values_metric_data.append(int(metric.value))
            else:
                values_metric_data.append(round(float(metric.value), 2))
        metric_data = dict(zip(key_metric_data, values_metric_data))

        additional_data = {
            'created_at': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
            'session_source_medium': row.dimension_values[1].value
        }
        row_data = {**dimension_data, **metric_data, **additional_data}
        source_data.append(row_data)
        current_record_index += 1
        if not os.path.exists(f"{file_overview_session_source_medium_dir_path}/{today_date}"):
            os.makedirs(f"{file_overview_session_source_medium_dir_path}/{today_date}")
        if len(source_data) == limit or current_record_index == total_record:
            transform_source = pd.DataFrame(data=source_data)
            transform_source.to_csv(
                f"{file_overview_session_source_medium_dir_path}/{today_date}/{file_index}.csv",
                index=False
            )
            file_index += 1
            source_data.clear()
    return True


def get_list_session_source_medium_files():
    result = glob.glob(f"{file_overview_session_source_medium_dir_path}/{today_date}/*.csv")
    return result


def get_list_session_source_medium_objects_gcs_to_bq():
    result = []
    storage_client = GCPConnectionInfo.storage_client
    gcs_bucket = storage_client.get_bucket(BUCKET)
    list_blobs = list(gcs_bucket.list_blobs(prefix=f'{store_directory}/overview_session/'))
    for blob in list_blobs:
        if blob.content_type == 'application/octet-stream':
            result.append(blob.name)
    return result








def remove_old_data():
    query = f'DELETE {GCPConnectionInfo.gcp_project}.{BQ_DATASET}.{bq_table_overview_session_source_medium} WHERE date >= "{last_30_day_ago}"'
    GCPConnectionInfo().execute_query(query=query)


dag = DAG(
    dag_id='ga4_gy_to_bq',
    schedule='0 */2 * * *',
    start_date=datetime(2024, 6, 1, tzinfo=local_tz),
    tags=['ga4_to_bigquery'],
    catchup=False,
    default_args=default_args
)

ga_overview_to_local = ShortCircuitOperator(
    task_id='ga_overview_to_local',
    python_callable=get_ga4_overview_medium_data_to_local,
    dag=dag
)

ga_overview_session_source_medium_to_local = ShortCircuitOperator(
    task_id='ga_overview_session_source_medium_to_local',
    python_callable=get_ga4_overview_session_source_medium_data_to_local,
    dag=dag
)

local_overview_to_gcs = LocalFilesystemToGCSOperator(
    task_id='local_overview_to_gcs',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    src=f"{file_overview_dir_path}/{today_date}.csv",
    dst=f"{store_directory}/overview/{today_date}.csv"
)

local_overview_session_source_medium_to_gcs = LocalFilesystemToGCSOperator(
    task_id='local_overview_session_source_medium_to_gcs',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    src=get_list_session_source_medium_files(),
    dst=f"{store_directory}/overview_session/"
)

delete_old_data = PythonOperator(
    task_id='delete_old_data',
    python_callable=remove_old_data,
    dag=dag
)

gcs_overview_to_bq = GCSToBigQueryOperator(
    task_id='gcs_overview_to_bq',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    source_objects=f"{store_directory}/overview/{today_date}.csv",
    destination_project_dataset_table=f"{BQ_DATASET}.{bq_table_overview}",
    schema_fields=[
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "DATE",
        },
        {
            "name": "sessions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "bounce_rate",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "total_revenue",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "total_users",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "new_users",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "transactions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "add_to_carts",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "created_at",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        }
    ],
    write_disposition='WRITE_TRUNCATE'
)

gcs_overview_session_source_medium_to_bq = GCSToBigQueryOperator(
    task_id='gcs_overview_session_source_medium_to_bq',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    source_objects=get_list_session_source_medium_objects_gcs_to_bq(),
    destination_project_dataset_table=f"{BQ_DATASET}.{bq_table_overview_session_source_medium}",
    schema_fields=[
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "DATE",
        },
        {
            "name": "sessions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "bounce_rate",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "total_revenue",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "total_users",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "new_users",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "transactions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "add_to_carts",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "created_at",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "session_source_medium",
            "mode": "NULLABLE",
            "type": "STRING",
        }
    ],
    write_disposition='WRITE_APPEND'
)

ga_overview_to_local >> local_overview_to_gcs >> gcs_overview_to_bq
ga_overview_session_source_medium_to_local >> local_overview_session_source_medium_to_gcs >> delete_old_data >> gcs_overview_session_source_medium_to_bq
