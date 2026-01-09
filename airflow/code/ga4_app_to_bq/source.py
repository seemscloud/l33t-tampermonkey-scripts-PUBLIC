import os
import pathlib
import pandas as pd
import glob
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
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

bq_table_daily_overview = 'daily_global_overview'
store_directory = 'app_ga4'
local_tz = timezone('Asia/Dubai')
today_date = datetime.now(tz=local_tz).strftime('%Y-%m-%d')
file_name = datetime.now(tz=local_tz).strftime('%Y-%m-%d') + '.csv'
directory_files_path = '/opt/airflow/files'
file_daily_overview_dir_path = directory_files_path + '/ga4/' + store_directory + '/' + bq_table_daily_overview
last_30_day_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
ga4_start_str = Variable.get('ga4_start_str') if Variable.get('ga4_start_str').lower() != "none" else last_30_day_ago
ga4_finish_str = Variable.get('ga4_finish_str') if Variable.get('ga4_finish_str').lower() != "none" else 'yesterday'
limit = 10000

# set bien moi truong de co the su dung BetaAnalyticsDataClient,
# co the set trong docker-compose or set tai phien lam viec ntn

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/airflow_service_account.json'

BQ_DATASET = 'app_ga4'
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

# property id ga4
property_id = "388272751"
client = BetaAnalyticsDataClient()


# get data follow field require
def run_report_daily_overview(offset=0):
    result = []
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name='country'),
            Dimension(name='platform')
        ],
        metrics=[
            Metric(name="addToCarts"),
            Metric(name="keyEvents:app_store_subscription_convert"),
            Metric(name="keyEvents:first_open"),
            #             Metric(name="keyEvents:purchase"),
            Metric(name="engagedSessions"),
            Metric(name="sessions"),
            Metric(name="totalRevenue"),
            Metric(name="transactions"),
            #             Metric(name="activeUsers"),
            Metric(name="totalUsers"),
            #             Metric(name="keyEvents:app_store_subscription_renew"),
            Metric(name="newUsers"),
            #             Metric(name="sessionKeyEventRate"),
            #             Metric(name="userKeyEventRate"),
        ],
        dimension_filter={
            "filter": {
                "field_name": "platform",
                "in_list_filter": {
                    "values": ["iOS", "Android"]
                }
            }
        },
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
        result += run_report_daily_overview(offset + limit)
    return result


# handle data get from API and export to a file csv
def get_ga4_data_daily_overview_to_local():
    source_data = []
    raw_data = run_report_daily_overview()

    file_index = 1
    current_record_index = 0
    total_record = len(raw_data)
    if len(raw_data) < 1:
        return False
    for row in raw_data:
        date_int = row.dimension_values[0].value
        date_str = datetime.strptime(date_int, '%Y%m%d').strftime('%Y-%m-%d')
        dimension_data = {
            'date': date_str,
            'country': row.dimension_values[1].value,
            'platform': row.dimension_values[2].value,

        }
        key_metric_data = [
            'add_to_carts',
            'conversions_app_store_subscription_convert',
            'conversions_first_open',
            #             'conversions_purchase',
            'engaged_sessions',
            'sessions',
            'total_revenue',
            'transactions',
            #             'active_users',
            'total_users',
            #             'conversions_app_store_subscription_renew',
            'new_users',
            #             'session_key_event_rate',
            #             'user_key_event_rate'
        ]
        values_metric_data = []
        for metric in row.metric_values:
            if metric.value.isdigit():
                values_metric_data.append(int(metric.value))
            else:
                values_metric_data.append(round(float(metric.value), 2))
        metric_data = dict(zip(key_metric_data, values_metric_data))

        row_data = {**dimension_data, **metric_data}
        source_data.append(row_data)
        current_record_index += 1
        if not os.path.exists(f"{file_daily_overview_dir_path}/{today_date}"):
            os.makedirs(f"{file_daily_overview_dir_path}/{today_date}")
        if len(source_data) == limit or current_record_index == total_record:
            transform_source = pd.DataFrame(data=source_data)
            transform_source.to_csv(
                f"{file_daily_overview_dir_path}/{today_date}/{file_index}.csv",
                index=False
            )
            file_index += 1
            source_data.clear()
    return True


def get_list_daily_overview_files():
    result = glob.glob(f"{file_daily_overview_dir_path}/{today_date}/*.csv")
    return result


def get_list_daily_overview_gcs_to_bq():
    result = []
    storage_client = GCPConnectionInfo.storage_client
    gcs_bucket = storage_client.get_bucket(BUCKET)
    list_blobs = list(gcs_bucket.list_blobs(prefix=f'{store_directory}/{bq_table_daily_overview}/'))
    for blob in list_blobs:
        if blob.content_type == 'application/octet-stream':
            result.append(blob.name)
    return result


def remove_daily_overview_csv_files():
    if os.path.exists(f"{file_daily_overview_dir_path}/{today_date}"):
        list_files = glob.glob(f"{file_daily_overview_dir_path}/{today_date}/*.csv")
        if list_files:
            for file_path in list_files:
                os.remove(file_path)


# build query de remove data theo condition
def remove_daily_overview_old_data():
    query = f'DELETE {GCPConnectionInfo.gcp_project}.{BQ_DATASET}.{bq_table_daily_overview} WHERE date >= "{last_30_day_ago}"'
    print(query)
    GCPConnectionInfo().execute_query(query=query)


dag = DAG(
    dag_id='ga4_app_to_bq',
    schedule='0 */6 * * *',
    start_date=datetime(2024, 11, 4, tzinfo=timezone('UTC')),
    tags=['ga4_to_bigquery'],
    catchup=False,
    default_args=default_args,
)

# get data from GA4 save in local project
ga_daily_overview_to_local = ShortCircuitOperator(
    task_id='ga_daily_overview_to_local',
    python_callable=get_ga4_data_daily_overview_to_local,
    dag=dag
)

# push data from local, push to bucket of Google Cloud Storage (GCS)
local_daily_overview_to_gcs = LocalFilesystemToGCSOperator(
    task_id='local_daily_overview_to_gcs',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    src=get_list_daily_overview_files(),
    dst=f"{store_directory}/{bq_table_daily_overview}/"
)

# remove data follow condition
remove_daily_overview_old_data = PythonOperator(
    task_id='remove_daily_overview_old_data',
    python_callable=remove_daily_overview_old_data,
    dag=dag
)

# push data from Google Cloud Storage (GCS) to Big Query
gcs_daily_overview_to_bq = GCSToBigQueryOperator(
    task_id='gcs_daily_overview_to_bq',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    source_objects=get_list_daily_overview_gcs_to_bq(),
    destination_project_dataset_table=f"{BQ_DATASET}.{bq_table_daily_overview}",
    schema_fields=[
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "DATE",
        },
        {
            "name": "country",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "platform",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "add_to_carts",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "conversions_app_store_subscription_convert",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "conversions_first_open",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        #         {
        #             "name": "conversions_purchase",
        #             "mode": "NULLABLE",
        #             "type": "INTEGER",
        #         },
        {
            "name": "engaged_sessions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "sessions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "total_revenue",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "transactions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        #         {
        #             "name": "active_users",
        #             "mode": "NULLABLE",
        #             "type": "INTEGER",
        #         },
        {
            "name": "total_users",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        #         {
        #          "name": "conversions_app_store_subscription_renew",
        #          "mode": "NULLABLE",
        #          "type": "FLOAT",
        #         },
        {
            "name": "new_users",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        #         {
        #         "name": "session_key_event_rate",
        #         "mode": "NULLABLE",
        #         "type": "FLOAT",
        #         },
        #         {
        #         "name": "user_key_event_rate",
        #         "mode": "NULLABLE",
        #         "type": "FLOAT",
        #         }
    ],
    write_disposition='WRITE_APPEND',
    # WRITE_APPEND add new record, WRITE_TRUNCATE remove all data and write , WRITE_EMPTY only write when table empty
)

delete_daily_overview_gcs_overview_object = GCSDeleteObjectsOperator(
    task_id='delete_daily_overview_gcs_overview_object',
    bucket_name=BUCKET,
    gcp_conn_id=GCP_CONN,
    objects=get_list_daily_overview_gcs_to_bq(),
    dag=dag
)

remove_daily_overview_after_transfer = PythonOperator(
    task_id='remove_daily_overview_after_transfer',
    python_callable=remove_daily_overview_csv_files,
    dag=dag
)

ga_daily_overview_to_local >> local_daily_overview_to_gcs >> remove_daily_overview_old_data >> gcs_daily_overview_to_bq >> delete_daily_overview_gcs_overview_object >> remove_daily_overview_after_transfer

# --------------------------------------------------HANDLE FOR DAILY OVERVIEW REVENUE------------------------------------------------------------------------------------------------------

bq_table_daily_overview_revenue = 'daily_global_overview_revenue'
file_daily_overview_revenue_dir_path = directory_files_path + '/ga4/' + store_directory + '/' + bq_table_daily_overview_revenue


# get data follow field require
def run_report_daily_overview_revenue(offset=0):
    result = []
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name='country'),
            Dimension(name='platform')
        ],
        metrics=[
            Metric(name="addToCarts"),
            Metric(name="checkouts"),
            Metric(name='ecommercePurchases'),
            Metric(name="totalPurchasers"),
            Metric(name="totalRevenue"),
            Metric(name='transactions'),
            Metric(name="keyEvents"),
            Metric(name='keyEvents:purchase'),
            Metric(name='keyEvents:in_app_purchase'),
        ],
        dimension_filter={
            "filter": {
                "field_name": "platform",
                "in_list_filter": {
                    "values": ["iOS", "Android"]
                }
            }
        },
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
        result += run_report_daily_overview_revenue(offset + limit)
    return result


# handle data get from API and export to a file csv
def get_ga4_data_daily_overview_revenue_to_local():
    source_data = []
    raw_data = run_report_daily_overview_revenue()

    file_index = 1
    current_record_index = 0
    total_record = len(raw_data)
    if len(raw_data) < 1:
        return False
    for row in raw_data:
        date_int = row.dimension_values[0].value
        date_str = datetime.strptime(date_int, '%Y%m%d').strftime('%Y-%m-%d')
        dimension_data = {
            'date': date_str,
            'country': row.dimension_values[1].value,
            'platform': row.dimension_values[2].value,

        }
        key_metric_data = ['add_to_carts', 'checkouts', 'ecommerce_purchases', 'total_purchasers', 'total_revenue',
                           'transactions',
                           'key_events', 'conversions_purchase', 'conversions_in_app_purchase']
        values_metric_data = []
        for metric in row.metric_values:
            if metric.value.isdigit():
                values_metric_data.append(int(metric.value))
            else:
                values_metric_data.append(round(float(metric.value), 2))
        metric_data = dict(zip(key_metric_data, values_metric_data))

        row_data = {**dimension_data, **metric_data}
        source_data.append(row_data)
        current_record_index += 1
        if not os.path.exists(f"{file_daily_overview_revenue_dir_path}/{today_date}"):
            os.makedirs(f"{file_daily_overview_revenue_dir_path}/{today_date}")
        if len(source_data) == limit or current_record_index == total_record:
            transform_source = pd.DataFrame(data=source_data)
            transform_source.to_csv(
                f"{file_daily_overview_revenue_dir_path}/{today_date}/{file_index}.csv",
                index=False
            )
            file_index += 1
            source_data.clear()
    return True


def get_list_daily_overview_revenue_files():
    result = glob.glob(f"{file_daily_overview_revenue_dir_path}/{today_date}/*.csv")
    return result


def get_list_daily_overview_revenue_gcs_to_bq():
    result = []
    storage_client = GCPConnectionInfo.storage_client
    gcs_bucket = storage_client.get_bucket(BUCKET)
    list_blobs = list(gcs_bucket.list_blobs(prefix=f'{store_directory}/{bq_table_daily_overview_revenue}/'))
    for blob in list_blobs:
        if blob.content_type == 'application/octet-stream':
            result.append(blob.name)
    return result


def remove_daily_overview_revenue_csv_files():
    if os.path.exists(f"{file_daily_overview_revenue_dir_path}/{today_date}"):
        list_files = glob.glob(f"{file_daily_overview_revenue_dir_path}/{today_date}/*.csv")
        if list_files:
            for file_path in list_files:
                os.remove(file_path)


# build query de remove data theo condition
def remove_daily_overview_revenue_old_data():
    query = f'DELETE {GCPConnectionInfo.gcp_project}.{BQ_DATASET}.{bq_table_daily_overview_revenue} WHERE date >= "{last_30_day_ago}"'
    print(query)
    GCPConnectionInfo().execute_query(query=query)


# get data from GA4 save in local project
ga_daily_overview_revenue_to_local = ShortCircuitOperator(
    task_id='ga_daily_overview_revenue_to_local',
    python_callable=get_ga4_data_daily_overview_revenue_to_local,
    dag=dag
)

# push data from local, push to bucket of Google Cloud Storage (GCS)
local_daily_overview_revenue_to_gcs = LocalFilesystemToGCSOperator(
    task_id='local_daily_overview_revenue_to_gcs',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    src=get_list_daily_overview_revenue_files(),
    dst=f"{store_directory}/{bq_table_daily_overview_revenue}/"
)

# remove data follow condition
remove_daily_overview_revenue_old_data = PythonOperator(
    task_id='remove_daily_overview_revenue_old_data',
    python_callable=remove_daily_overview_revenue_old_data,
    dag=dag
)

# push data from Google Cloud Storage (GCS) to Big Query
gcs_daily_overview_revenue_to_bq = GCSToBigQueryOperator(
    task_id='gcs_daily_overview_revenue_to_bq',
    gcp_conn_id=GCP_CONN,
    bucket=BUCKET,
    source_objects=get_list_daily_overview_revenue_gcs_to_bq(),
    destination_project_dataset_table=f"{BQ_DATASET}.{bq_table_daily_overview_revenue}",
    schema_fields=[
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "DATE",
        },
        {
            "name": "country",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "platform",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "add_to_carts",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "checkouts",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "ecommerce_purchases",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "total_purchasers",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "total_revenue",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "transactions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "key_events",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "conversions_purchase",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "conversions_in_app_purchase",
            "mode": "NULLABLE",
            "type": "FLOAT",
        }
    ],
    write_disposition='WRITE_APPEND',
    # WRITE_APPEND add new record, WRITE_TRUNCATE remove all data and write , WRITE_EMPTY only write when table empty
)

delete_daily_overview_revenue_gcs_overview_object = GCSDeleteObjectsOperator(
    task_id='delete_daily_overview_revenue_gcs_overview_object',
    bucket_name=BUCKET,
    gcp_conn_id=GCP_CONN,
    objects=get_list_daily_overview_revenue_gcs_to_bq(),
    dag=dag
)

remove_daily_overview_revenue_after_transfer = PythonOperator(
    task_id='remove_daily_overview_revenue_after_transfer',
    python_callable=remove_daily_overview_revenue_csv_files,
    dag=dag
)

ga_daily_overview_revenue_to_local >> local_daily_overview_revenue_to_gcs >> remove_daily_overview_revenue_old_data >> gcs_daily_overview_revenue_to_bq >> delete_daily_overview_revenue_gcs_overview_object >> remove_daily_overview_revenue_after_transfer

if __name__ == '__main__':
    # run_report_daily_overview()
    get_ga4_data_daily_overview_to_local()
