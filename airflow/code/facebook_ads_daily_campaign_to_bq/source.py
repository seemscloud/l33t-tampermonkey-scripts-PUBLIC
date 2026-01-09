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
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adsinsights import AdsInsights

last_9_day_ago = (datetime.today() - timedelta(days=9)).strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = datetime.today().strftime('%Y-%m-%d')
bq_dataset = 'ad_fb'
bq_table = 'daily_campaign'
gcp_conn = GCPConnectionInfo.gcp_conn
project_id = GCPConnectionInfo.gcp_project
bucket = 'ads_facebook_airflow'
client = GCPConnectionInfo.bigquery_client

access_token = pandas.read_json('/opt/airflow/config/facebook_api_authorize.json', typ='series').access_token

local_tz = timezone('UTC')
email_on_failure = Variable.get("email_on_failure").rstrip(',').split(',')
default_args = {
    "email": email_on_failure,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "max_retry_delay": timedelta(hours=1),
}


def get_list_last_9_days_ago():
    result = []
    today = datetime.today()
    for d in range(9):
        day = (today - timedelta(days=d)).strftime('%Y-%m-%d')
        result.append(day)
    return result


def get_facebook_data_to_local():
    account_ids = Variable.get("facebook_ad_account_ids").rstrip(',').split(',')
    list_last_9_days_ago = get_list_last_9_days_ago()
    df_data = []
    FacebookAdsApi.init(access_token=access_token)

    insight_fields = [
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.account_currency,
        AdsInsights.Field.account_id,
        AdsInsights.Field.account_name,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.clicks,
        AdsInsights.Field.frequency,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.spend,
        AdsInsights.Field.date_start,
        AdsInsights.Field.date_stop
    ]

    for account_id in account_ids:
        for date_range in list_last_9_days_ago:
            params = {
                "time_range": {
                    'since': date_range,
                    'until': date_range
                },
                'level': 'campaign'

            }
            account = AdAccount(account_id)
            insights = account.get_insights(fields=insight_fields, params=params)
            for insight in insights:
                df_row_data = {
                    "_fivetran_id": None,
                    "campaign_id": insight.get('campaign_id'),
                    "date": insight.get('date_start'),
                    "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "account_currency": insight.get('account_currency'),
                    "account_id": insight.get('account_id'),
                    "account_name": insight.get('account_name'),
                    "campaign_name": insight.get('campaign_name'),
                    "clicks": insight.get('clicks'),
                    "frequency": round(float(insight.get('frequency')), 6),
                    "impressions": insight.get('impressions'),
                    "reach": insight.get('reach'),
                    "spend": round(float(insight.get('spend')), 2),
                }
                df_data.append(df_row_data)
    if df_data:
        if not os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}'):
            os.makedirs(f'/opt/airflow/files/{bq_dataset}/{bq_table}')
        df = pandas.DataFrame(data=df_data, dtype=object)
        df = df.replace(r'\r+|\n+|\t+', '', regex=True)
        df.to_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv', sep=';', index=False)
        return True
    return False


def remove_old_data():
    query = f'DELETE {GCPConnectionInfo.gcp_project}.{bq_dataset}.{bq_table} WHERE date > "{last_9_day_ago}"'
    GCPConnectionInfo().execute_query(query=query)


def remove_local_file():
    if os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/'):
        list_files = glob.glob(f'/opt/airflow/files/{bq_dataset}/{bq_table}/*.csv')
        if list_files:
            for file_path in list_files:
                os.remove(file_path)


dag = DAG(
    dag_id='facebook_ads_daily_campaign_to_bq',
    schedule='45 0/6 * * *',
    start_date=datetime(2024, 11, 27, tzinfo=local_tz),
    tags=['facebook_ads'],
    catchup=False,
    default_args=default_args
)

fb_ads_to_local = ShortCircuitOperator(
    task_id='fb_ads_to_local',
    python_callable=get_facebook_data_to_local,
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
            "name": "campaign_id",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "DATE",
        },
        {
            "name": "_fivetran_synced",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "account_currency",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "account_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "account_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "campaign_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "clicks",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "frequency",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "impressions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "reach",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "spend",
            "mode": "NULLABLE",
            "type": "FLOAT",
        }
    ],
    dag=dag
)

delete_local_file = PythonOperator(
    task_id='delete_local_file',
    python_callable=remove_local_file,
    dag=dag
)

fb_ads_to_local >> local_data_to_gcs >> delete_old_data >> gcs_data_to_bq >> delete_local_file
