import json
import requests
import os
import glob
import pandas
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from pytz import timezone
from datetime import datetime, timedelta
from gcp_connection.connection_info import GCPConnectionInfo

last_30_day_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = datetime.today().strftime('%Y-%m-%d')
bq_dataset = 'ads_criteo'
bq_table = 'adset_statistics_hourly_report'
gcp_conn = GCPConnectionInfo.gcp_conn
bucket = 'criteo_ads_airflow'
client = GCPConnectionInfo.bigquery_client

local_tz = timezone('UTC')
email_on_failure = Variable.get("email_on_failure").rstrip(',').split(',')
default_args = {
    "email": email_on_failure,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
    "max_retry_delay": timedelta(hours=1),
}


def get_access_token():
    url = 'https://api.criteo.com/oauth2/token'
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    criteo_api_authorize = pandas.read_json('/opt/airflow/config/criteo_api_authorize.json', typ='series')
    response = requests.post(
        url=url,
        headers=headers,
        data={
            "grant_type": "refresh_token",
            "client_id": criteo_api_authorize.client_id,
            "client_secret": criteo_api_authorize.client_secret,
            "refresh_token": criteo_api_authorize.refresh_token
        }
    )
    response_data = json.loads(response.text)
    return response_data.get('access_token')


def get_criteo_data_to_local():
    df_data = []
    access_token = get_access_token()
    url = 'https://api.criteo.com/2024-07/statistics/report'
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        'Authorization': f'Bearer {access_token}',
    }
    payload = json.dumps({
        "startDate": last_30_day_ago,
        "endDate": yesterday,
        "format": "json",
        "dimensions": ["AdsetId", "Day", "CategoryId", "Hour", "AdvertiserId"],
        "metrics": ["Displays", "Clicks", "RevenueGeneratedPc30d", "SalesPc30d", "Reach", "AdvertiserCost",
                    "Audience"],
        "timezone": "UTC",
        "currency": "EUR"
    })
    response = requests.post(
        url=url,
        headers=header,
        data=payload
    )
    response_data = json.loads(response.text)
    if response_data['Rows']:
        for row in response_data['Rows']:
            df_row_data = {
                "_fivetran_id": None,
                "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "adset_id": row.get('AdsetId', None),
                "adset_name": row.get('Adset', None),
                "advertiser_cost": round(float(row.get('AdvertiserCost', None)), 4),
                "advertiser_id": row.get('AdvertiserId', None),
                "audience": row.get('Audience', None),
                "category_id": row.get('CategoryId', None),
                "category_name": row.get('Category', None),
                "clicks": row.get('Clicks', None),
                "currency": row.get('Currency', None),
                "date": datetime.strptime(row.get('Hour', None), '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
                "displays": row.get('Displays', None),
                "reach": round(float(row.get('Reach', None)), 4),
                "revenue_generated_pc_30_d": round(float(row.get('RevenueGeneratedPc30d', None)), 4),
                "sales_pc_30_d": row.get('SalesPc30d', None),
            }
            df_data.append(df_row_data)
        if not os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}'):
            os.makedirs(f'/opt/airflow/files/{bq_dataset}/{bq_table}')
        df = pandas.DataFrame(data=df_data, dtype=object)
        df = df.replace(r'\r+|\n+|\t+', '', regex=True)
        df.to_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv', sep=';', index=False)
        return True
    return False


def execute_query(query):
    try:
        query_job = GCPConnectionInfo.bigquery_client.query(query=query)
    except ValueError as e:
        return False
    return query_job.result()


def remove_old_data():
    query = f'DELETE {GCPConnectionInfo.gcp_project}.{bq_dataset}.{bq_table} WHERE date >= "{last_30_day_ago}"'
    execute_query(query=query)


def remove_local_file():
    if os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/'):
        list_files = glob.glob(f'/opt/airflow/files/{bq_dataset}/{bq_table}/*.csv')
        if list_files:
            for file_path in list_files:
                os.remove(file_path)


dag = DAG(
    dag_id='criteo_ads_adset_statistics_hourly_report_to_bq',
    schedule='15 */6 * * *',
    start_date=datetime(2024, 10, 8, tzinfo=local_tz),
    tags=['criteo_ads'],
    catchup=False,
    default_args=default_args
)

criteo_ads_to_local = ShortCircuitOperator(
    task_id='criteo_ads_to_local',
    python_callable=get_criteo_data_to_local,
    dag=dag
)

local_data_to_gcs = LocalFilesystemToGCSOperator(
    task_id='local_data_to_gcs',
    gcp_conn_id=gcp_conn,
    src=f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv',
    dst=f'{bq_table}/{today_date}',
    bucket=bucket,
    dag=dag
)

delete_old_data = PythonOperator(
    task_id='delete_old_data',
    python_callable=remove_old_data,
    dag=dag
)

gcs_data_to_bq = GCSToBigQueryOperator(
    task_id='gcs_data_to_bq',
    gcp_conn_id=gcp_conn,
    bucket=bucket,
    source_objects=f'{bq_table}/{today_date}',
    destination_project_dataset_table=f'{bq_dataset}.{bq_table}',
    write_disposition='WRITE_APPEND',
    field_delimiter=';',
    schema_fields=[
        {
            "name": "_fivetran_id",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "_fivetran_synced",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "adset_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "adset_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "advertiser_cost",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "advertiser_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "audience",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "category_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "category_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "clicks",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "currency",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "displays",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "reach",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "revenue_generated_pc_30_d",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "sales_pc_30_d",
            "mode": "NULLABLE",
            "type": "INTEGER",
        }
    ],
    dag=dag
)

delete_local_file = PythonOperator(
    task_id='delete_local_file',
    python_callable=remove_local_file,
    dag=dag
)

criteo_ads_to_local >> local_data_to_gcs >> delete_old_data >> gcs_data_to_bq >> delete_local_file

if __name__ == '__main__':
    # remove_old_data()
    get_criteo_data_to_local()
