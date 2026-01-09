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
bq_table = 'audience'
gcp_conn = GCPConnectionInfo.gcp_conn
bucket = 'criteo_ads_airflow'
client = GCPConnectionInfo.bigquery_client

local_tz = timezone('UTC')
email_on_failure = Variable.get("email_on_failure").rstrip(',').split(',')
default_args = {
    "email": email_on_failure,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "max_retry_delay": timedelta(minutes=15),
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


def get_advertiser_ids(access_token):
    result = []
    url = "https://api.criteo.com/2024-07/advertisers/me"
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        'Authorization': f'Bearer {access_token}',
    }

    response = requests.get(
        url=url,
        headers=header,
    )
    response_data = json.loads(response.text)
    if response_data['data']:
        for row in response_data['data']:
            result.append(row.get('id'))
    return result


def get_raw_data(advertiser_ids, access_token, offset=0):
    result = []
    url = "https://api.criteo.com/2024-07/marketing-solutions/audience-segments/search"
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        'Authorization': f'Bearer {access_token}',
    }

    payload = json.dumps({
        "data": {
            "attributes": {
                "advertiserIds": advertiser_ids
            }
        }
    })
    response = requests.post(
        url=url,
        headers=header,
        data=payload,
        params={
            "limit": 100,
            "offset": offset,
        }
    )
    response_data = json.loads(response.text)
    if response_data.get('data', None) is not None:
        result += response_data.get('data')
    if response_data.get('meta'):
        if response_data.get('meta').get('totalItems') > offset + 100:
            result += get_raw_data(advertiser_ids, access_token, offset + 100)
    return result


def get_criteo_data_to_local():
    df_data = []
    access_token = get_access_token()
    advertiser_ids = get_advertiser_ids(access_token)
    response_data = get_raw_data(advertiser_ids, access_token)
    if response_data:
        for row in response_data:
            attribute_data = row.get('attributes')
            df_row_data = {
                "id": row.get("id"),
                "_fivetran_deleted": False,
                "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "advertiser_id": attribute_data.get('advertiserId'),
                "created_at": attribute_data.get('createdAt'),
                "description": attribute_data.get("description"),
                "name": attribute_data.get("name"),
                "nb_lines": None,
                "nb_lines_email": None,
                "nb_matches_email": None,
                "updated_at": attribute_data.get("updatedAt"),
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
    ids = pandas.read_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv',
                          sep=';').id.to_list()
    audience_ids = []
    for audience_id in ids:
        audience_ids.append(str(audience_id))
    execute_query(
        f'DELETE FROM {GCPConnectionInfo.gcp_project}.{bq_dataset}.{bq_table} WHERE id IN ({",".join(audience_ids)})'
    )


def remove_local_file():
    if os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/'):
        list_files = glob.glob(f'/opt/airflow/files/{bq_dataset}/{bq_table}/*.csv')
        if list_files:
            for file_path in list_files:
                os.remove(file_path)


dag = DAG(
    dag_id='criteo_ads_audience_to_bq',
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
            "name": "id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "_fivetran_deleted",
            "mode": "NULLABLE",
            "type": "BOOLEAN",
        },
        {
            "name": "_fivetran_synced",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "advertiser_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "created_at",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "description",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "nb_lines",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "nb_lines_email",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "nb_matches_email",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "updated_at",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
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
    # print(get_advertiser_ids())
    get_criteo_data_to_local()
