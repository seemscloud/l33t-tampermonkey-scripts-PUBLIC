import json
import requests
import os
import glob
import pandas
from airflow import DAG
from functools import reduce
import numpy
import operator
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from pytz import timezone
from datetime import datetime, timedelta
from gcp_connection.connection_info import GCPConnectionInfo

today_date = datetime.today().strftime('%Y-%m-%d')
bq_dataset = 'ads_criteo'
bq_table = 'ad_set'
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


def get_data_from_nested_dict(data_dict, *keys):
    for key in keys:
        try:
            data_dict = data_dict[key]
        # except KeyError:
        #     return None
        except TypeError:
            return None
    return data_dict


def get_criteo_data_to_local():
    df_data = []
    access_token = get_access_token()
    advertiser_ids = get_advertiser_ids(access_token)
    url = "https://api.criteo.com/2024-07/marketing-solutions/ad-sets/search"
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        'Authorization': f'Bearer {access_token}',
    }
    payload = json.dumps({
        "filters": {
            "advertiserIds": advertiser_ids
        }
    })
    response = requests.post(
        url=url,
        headers=header,
        data=payload
    )
    response_data = json.loads(response.text)
    if response_data.get('data', None) is not None:
        for row in response_data.get('data'):
            attribute_data = row.get('attributes')
            bidding = attribute_data.get('bidding', {})
            budget = attribute_data.get('budget', {})
            targeting = attribute_data.get('targeting', {})
            frequency_capping = targeting.get('frequencyCapping', {})
            schedule = attribute_data.get('schedule', {})
            df_row_data = {
                "id": row.get('id'),
                "_fivetran_deleted": False,
                "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "advertiser_id": attribute_data.get('advertiserId'),
                "bidding_bid_amount": bidding.get('bidAmount', None),
                "bidding_bid_strategy": bidding.get('bidStrategy', None),
                "bidding_cost_controller": bidding.get('costController', None),
                "budget_amount": budget.get('budgetAmount', None),
                "budget_delivery_smoothing": budget.get('budgetDeliverySmoothing', None),
                "budget_delivery_week": budget.get("budgetDeliveryWeek", None),
                "budget_renewal": budget.get('budgetRenewal', None),
                "budget_strategy": budget.get('budgetStrategy', None),
                "campaign_id": attribute_data.get('campaignId'),
                "destination_environment": attribute_data.get('destinationEnvironment', None),
                "frequency_capping_frequency": frequency_capping.get(
                    'frequency') if frequency_capping is not None else None,
                "frequency_capping_maximum_impressions": int(frequency_capping.get(
                    'maximumImpressions')) if frequency_capping is not None else None,
                "geolocation_countries_operand": get_data_from_nested_dict(targeting, 'geoLocation', 'countries',
                                                                           'value', 'operand'),
                "geolocation_subdivisions_operand": get_data_from_nested_dict(targeting, 'geoLocation', 'subdivisions',
                                                                              'value', 'operand'),
                "geolocation_zip_codes_operand": get_data_from_nested_dict(targeting, 'geoLocation', 'zipCodes',
                                                                           'value', 'operand'),
                "media_type": attribute_data.get('mediaType'),
                "name": attribute_data.get('name'),
                "schedule_activation_status": schedule.get('activationStatus', None),
                "schedule_delivery_status": schedule.get('deliveryStatus', None),
                "schedule_end_date": schedule.get('endDate', {}).get('value', None),
                "schedule_start_date": schedule.get('startDate', {}).get('value', None),
            }
            df_data.append(df_row_data)
        if not os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}'):
            os.makedirs(f'/opt/airflow/files/{bq_dataset}/{bq_table}')
        df = pandas.DataFrame(data=df_data, dtype='object')
        df = df.replace(r'\r+|\n+|\t+', '', regex=True)
        df['frequency_capping_maximum_impressions'] = df['frequency_capping_maximum_impressions'].astype('Int64')
        df.to_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv', sep=';', index=False)
        return True
    return False


def remove_old_data():
    return True


def remove_local_file():
    if os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/'):
        list_files = glob.glob(f'/opt/airflow/files/{bq_dataset}/{bq_table}/*.csv')
        if list_files:
            for file_path in list_files:
                os.remove(file_path)


dag = DAG(
    dag_id='criteo_ads_ad_set_to_bq',
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
    write_disposition='WRITE_TRUNCATE',
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
            "name": "bidding_bid_amount",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "bidding_bid_strategy",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "bidding_cost_controller",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "budget_amount",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "budget_delivery_smoothing",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "budget_delivery_week",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "budget_renewal",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "budget_strategy",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "campaign_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "destination_environment",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "frequency_capping_frequency",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "frequency_capping_maximum_impressions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "geolocation_countries_operand",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "geolocation_subdivisions_operand",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "geolocation_zip_codes_operand",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "media_type",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "schedule_activation_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "schedule_delivery_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "schedule_end_date",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "schedule_start_date",
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
    get_criteo_data_to_local()
    # print(get_access_token())
