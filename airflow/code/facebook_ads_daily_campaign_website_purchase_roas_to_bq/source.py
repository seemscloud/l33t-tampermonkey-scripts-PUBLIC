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
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adsactionstats import AdsActionStats

last_9_day_ago = (datetime.today() - timedelta(days=9)).strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = datetime.today().strftime('%Y-%m-%d')
bq_dataset = 'ad_fb'
bq_table = 'daily_campaign_website_purchase_roas'
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


def get_fb_ads_raw_data():
    campaign_ids = Variable.get("facebook_ad_campaign_ids").rstrip(',').split(',')
    result = []
    list_last_9_days_ago = get_list_last_9_days_ago()
    FacebookAdsApi.init(access_token=access_token)
    insight_fields = [
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.website_purchase_roas,
    ]
    for campaign_id in campaign_ids:
        for date_range in list_last_9_days_ago:
            params = {
                "time_range": {
                    "since": date_range,
                    "until": date_range
                },
                "level": AdsInsights.Level.campaign,
                "summary_action_breakdowns": [
                    AdsInsights.ActionBreakdowns.action_type
                ],
                "action_attribution_windows": [
                    AdsActionStats.Field.field_1d_view,
                    AdsActionStats.Field.field_7d_click,
                ],
            }
            campaign = Campaign(campaign_id)
            insights = campaign.get_insights(fields=insight_fields, params=params)
            if insights:
                result += insights
    return result


def get_facebook_data_to_local():
    raw_data = get_fb_ads_raw_data()
    df_data = []
    if raw_data:
        for insight in raw_data:
            values = insight.get('website_purchase_roas', [])
            if not values:
                continue
            index = 0
            for insight_website_purchase_roas in values:
                df_row_data = {
                    "_fivetran_id": None,
                    "campaign_id": insight.get('campaign_id'),
                    "date": insight.get('date_start'),
                    "index": index,
                    "_1_d_view": insight_website_purchase_roas.get('1d_view'),
                    "_7_d_click": insight_website_purchase_roas.get('7d_click'),
                    "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "action_type": insight_website_purchase_roas.get('action_type'),
                    "value": insight_website_purchase_roas.get('value'),
                }
                index += 1
                df_data.append(df_row_data)
        if not df_data:
            return False
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
    dag_id='facebook_ads_daily_campaign_website_purchase_roas_to_bq',
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
            "name": "index",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "_1_d_view",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "_7_d_click",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "_fivetran_synced",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "action_type",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "value",
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
