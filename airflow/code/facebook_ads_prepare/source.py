import json
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from pytz import timezone
from datetime import datetime, timedelta
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
import pandas

last_9_day_ago = (datetime.today() - timedelta(days=9)).strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = datetime.today().strftime('%Y-%m-%d')
bucket = 'ads_facebook_airflow'
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


def get_facebook_ad_account_ids():
    FacebookAdsApi.init(access_token=access_token)
    url = "https://graph.facebook.com/v21.0/286709921951730/owned_ad_accounts"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(
        url=url,
        headers=headers,
        params={
            "limit": 250
        }
    )
    response_data = json.loads(response.text).get('data', [])
    campaign_fields = [
        'name',
        'status'
    ]
    campaign_params = {
        'status': ['ACTIVE'],
        'effective_status': ['ACTIVE'],
        'configured_status': ['ACTIVE'],

    }

    ad_account_ids = []
    if response_data:
        for ad_account_data in response_data:
            campaigns = AdAccount(ad_account_data.get('id')).get_campaigns(
                fields=campaign_fields,
                params=campaign_params
            )
            if campaigns and int(ad_account_data.get('account_id')) != 1739250160219057:
                ad_account_ids.append(ad_account_data.get('id'))
        Variable.set('facebook_ad_account_ids', ','.join(ad_account_ids))


def get_facebook_campaign_ids():
    ad_account_ids = (Variable.get('facebook_ad_account_ids')).split(',')
    FacebookAdsApi.init(access_token=access_token)
    campaign_fields = [
        'name',
        'account_id',
        'status',
        'budget_remaining',
        'campaign_group_active_time',
        'configured_status',
        'is_budget_schedule_enabled',
        'lifetime_budget',
        'start_time',
        'stop_time',
        'last_budget_toggling_time'
    ]

    facebook_ad_campaign_ids = []
    for ad_account_id in ad_account_ids:
        ad_account_campaigns = AdAccount(ad_account_id).get_campaigns(
            fields=campaign_fields,
        )

        for campaign in ad_account_campaigns:
            stop_time_str = campaign.get('stop_time', '1970-01-01T00:00:00+0000')
            stop_time = datetime.strptime(stop_time_str, "%Y-%m-%dT%H:%M:%S%z")
            if (datetime.now(stop_time.tzinfo) - timedelta(days=9)).timestamp() <= stop_time.timestamp():
                is_stopped = False
            else:
                is_stopped = True
            if (campaign.get('stop_time', None) is None or is_stopped is False) and campaign.get(
                    'status') == 'ACTIVE':
                facebook_ad_campaign_ids.append(campaign.get('id'))
    if facebook_ad_campaign_ids:
        Variable.set('facebook_ad_campaign_ids', ','.join(facebook_ad_campaign_ids))


dag = DAG(
    dag_id='facebook_ads_prepare',
    schedule='15 0 * * *',
    start_date=datetime(2024, 11, 27, tzinfo=local_tz),
    tags=['facebook_ads'],
    catchup=False,
    default_args=default_args
)

prepare_ad_account_id = PythonOperator(
    task_id='prepare_ad_account_id',
    python_callable=get_facebook_ad_account_ids,
    dag=dag
)

prepare_ad_campaign_id = PythonOperator(
    task_id='prepare_ad_campaign_id',
    python_callable=get_facebook_campaign_ids,
    dag=dag
)

prepare_ad_account_id >> prepare_ad_campaign_id
