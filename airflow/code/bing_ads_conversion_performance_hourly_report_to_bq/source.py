"""
    https://learn.microsoft.com/en-us/advertising/reporting-service/conversionperformancereportrequest?view=bingads-13&tabs=json
"""

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
from bing_api_extend.oauth import BingOAuthHelper
import zipfile as zp

last_7_day_ago = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = datetime.today().strftime('%Y-%m-%d')
bq_project = GCPConnectionInfo.gcp_project_test
bq_dataset = 'ads_msft'
bq_table = 'conversion_performance_hourly_report'
gcp_conn = GCPConnectionInfo.gcp_conn_test
bucket = 'bing_ads_airflow'
client = GCPConnectionInfo.bigquery_client
access_token = BingOAuthHelper().get_access_token()
developer_token = pandas.read_json('/opt/airflow/config/bing_ads_api_authorize.json', typ='series').developer_token

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


def get_files_path_in_dir(dir_path):
    return ''.join(glob.glob(f"{dir_path}/*.csv"))


def get_ad_account_ids():
    account_ids = []
    url = "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13/AccountsInfo/Query"
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DeveloperToken": developer_token,
        'Authorization': f'Bearer {access_token}',
    }
    payload = json.dumps({
        "CustomerId": 252231699
    })
    response = requests.post(
        url=url,
        headers=header,
        data=payload
    )
    if response.status_code == 200:
        accounts_info = json.loads(response.text).get('AccountsInfo')
        for account in accounts_info:
            account_ids.append(account.get('Id'))
        return account_ids
    else:
        raise Exception(f'Error when make request!!. Detail: {response.text}')


def get_bing_ads_report_request_id():
    ad_account_ids = get_ad_account_ids()
    url = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit"
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DeveloperToken": developer_token,
        'Authorization': f'Bearer {access_token}',
        "CustomerAccountId": ','.join(ad_account_ids),
        "CustomerId": '252231699'
    }
    payload = json.dumps({
        "ReportRequest":
            {
                "ExcludeReportFooter": True,
                "ExcludeReportHeader": True,
                "ReturnOnlyCompleteData": False,
                "Type": "ConversionPerformanceReportRequest",
                "Aggregation": "Hourly",
                "Columns": [
                    "AccountId",
                    "AccountName",
                    "AccountNumber",
                    "AccountStatus",
                    "AdGroupId",
                    "AdGroupName",
                    "AdGroupStatus",
                    "AllConversionRate",
                    "AllConversions",
                    "AllConversionsQualified",
                    "AllCostPerConversion",
                    "AllReturnOnAdSpend",
                    "AllRevenue",
                    "AllRevenuePerConversion",
                    "Assists",
                    "CampaignId",
                    "CampaignName",
                    "CampaignStatus",
                    "Clicks",
                    "ConversionRate",
                    "Conversions",
                    "ConversionsQualified",
                    "CostPerAssist",
                    "CostPerConversion",
                    "Ctr",
                    "TimePeriod",
                    "DeviceType",
                    "Goal",
                    "GoalId",
                    "GoalType",
                    "Impressions",
                    "Keyword",
                    "KeywordId",
                    "KeywordStatus",
                    "ReturnOnAdSpend",
                    "Revenue",
                    "RevenuePerAssist",
                    "RevenuePerConversion",
                    "Spend",
                    "ViewThroughConversions",
                    "ViewThroughRevenue",
                ],
                "Time": {
                    "PredefinedTime": "Last30Days",
                    "ReportTimeZone": "AmsterdamBerlinBernRomeStockholmVienna"
                },
                "Scope": {
                    "AccountIds": ad_account_ids
                }
            }
    })

    response = requests.post(
        url=url,
        headers=header,
        data=payload
    )
    if response.status_code == 200:
        response_data = json.loads(response.text)
        return response_data.get('ReportRequestId')
    else:
        raise Exception(f'Error when make request. Detail {response.text}')


def get_report_request_url(try_num=0):
    ad_account_ids = get_ad_account_ids()
    report_request_id = get_bing_ads_report_request_id()
    url = 'https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll'
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DeveloperToken": developer_token,
        'Authorization': f'Bearer {access_token}',
        "CustomerAccountId": ','.join(ad_account_ids),
        "CustomerId": '252231699'
    }

    payload = json.dumps({
        "ReportRequestId": report_request_id
    })
    response = requests.post(
        url=url,
        headers=header,
        data=payload
    )

    response_data = json.loads(response.text)
    if response.status_code == 200:
        if response_data.get('ReportRequestStatus').get('ReportDownloadUrl') is not None:
            return response_data.get('ReportRequestStatus').get('ReportDownloadUrl')
        elif try_num < 5:
            return get_report_request_url(try_num + 1)
        else:
            raise Exception(f'Error when make request!!. Detail: {response.text}')
    else:
        raise Exception(f'Error when make request!!. Detail: {response.text}')


def get_bing_ads_data_to_local():
    df_data = []
    report_download_url = get_report_request_url()
    response = requests.get(report_download_url)
    if not os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/extract'):
        os.makedirs(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/extract')
    with open(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/compress.zip', mode='wb') as file:
        file.write(response.content)
    with zp.ZipFile(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/compress.zip', 'r') as zip_ref:
        zip_ref.extractall(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/extract')
    extract_file_path = get_files_path_in_dir(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/extract')
    raw_data = pandas.read_csv(extract_file_path).to_dict(orient='records')
    if raw_data:
        for row in raw_data:
            df_row_data = {
                "_fivetran_id": None,
                "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "account_id": row.get("AccountId"),
                "account_name": row.get("AccountName"),
                "account_number": row.get("AccountNumber"),
                "account_status": row.get("AccountStatus"),
                "ad_group_id": row.get("AdGroupId"),
                "ad_group_name": row.get("AdGroupName"),
                "ad_group_status": row.get("AdGroupStatus"),
                "all_conversion_rate": row.get("AllConversionRate"),
                "all_conversions": row.get("AllConversions"),
                "all_conversions_qualified": row.get("AllConversionsQualified"),
                "all_cost_per_conversion": row.get("AllCostPerConversion"),
                "all_return_on_ad_spend": row.get("AllReturnOnAdSpend"),
                "all_revenue": row.get("AllRevenue"),
                "all_revenue_per_conversion": row.get("AllRevenuePerConversion"),
                "assists": row.get("Assists"),
                "campaign_id": row.get("CampaignId"),
                "campaign_name": row.get("CampaignName"),
                "campaign_status": row.get("CampaignStatus"),
                "clicks": row.get("Clicks"),
                "conversion_rate": row.get("ConversionRate"),
                "conversions": row.get("Conversions"),
                "conversions_qualified": row.get("ConversionsQualified"),
                "cost_per_assist": row.get("CostPerAssist"),
                "cost_per_conversion": row.get("CostPerConversion"),
                "ctr": row.get("Ctr"),
                "date": row.get("TimePeriod"),
                "device_type": row.get("DeviceType"),
                "goal": row.get("Goal"),
                "goal_id": row.get("GoalId"),
                "goal_type": row.get("GoalType"),
                "impressions": row.get("Impressions"),
                "keyword": row.get("Keyword"),
                "keyword_id": row.get("KeywordId"),
                "keyword_status": row.get("KeywordStatus"),
                "return_on_ad_spend": row.get("ReturnOnAdSpend"),
                "revenue": row.get("Revenue"),
                "revenue_per_assist": row.get("RevenuePerAssist"),
                "revenue_per_conversion": row.get("RevenuePerConversion"),
                "spend": row.get("Spend"),
                "view_through_conversions": row.get("ViewThroughConversions"),
                "view_through_revenue": row.get("ViewThroughRevenue"),
            }
            df_data.append(df_row_data)
        if not os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}'):
            os.makedirs(f'/opt/airflow/files/{bq_dataset}/{bq_table}')
        df = pandas.DataFrame(data=df_data, dtype=object)
        df = df.replace(r'\r+|\n+|\t+', '', regex=True)
        df.to_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/transformed.csv', sep=';', index=False)
        return True
    return False


def remove_old_data():
    GCPConnectionInfo().execute_query(
        f'DELETE FROM {bq_project}.{bq_dataset}.{bq_table} WHERE date >= "{last_7_day_ago}"')
    return True


def remove_local_file():
    if os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}'):
        list_csv_files = glob.glob(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/*.csv')
        if list_csv_files:
            for file_path in list_csv_files:
                os.remove(file_path)
        os.remove(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/compress.zip')
    if os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/extract'):
        list_extract_files = glob.glob(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/extract/*.csv')
        if list_extract_files:
            for file_path in list_extract_files:
                os.remove(file_path)


dag = DAG(
    dag_id='bing_ads_conversion_performance_hourly_report_to_bq',
    schedule='15 0 * * *',
    start_date=datetime(2024, 12, 16, tzinfo=local_tz),
    tags=['bing_ads'],
    catchup=False,
    default_args=default_args
)

bing_ads_to_local = ShortCircuitOperator(
    task_id='bing_ads_to_local',
    python_callable=get_bing_ads_data_to_local,
    dag=dag
)

local_data_to_gcs = LocalFilesystemToGCSOperator(
    task_id='local_data_to_gcs',
    gcp_conn_id=gcp_conn,
    src=f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}/transformed.csv',
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
            "name": "account_number",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "account_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "ad_group_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "ad_group_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "ad_group_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "all_conversion_rate",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "all_conversions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "all_conversions_qualified",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "all_cost_per_conversion",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "all_return_on_ad_spend",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "all_revenue",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "all_revenue_per_conversion",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "assists",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "campaign_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "campaign_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "campaign_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "clicks",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "conversion_rate",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "conversions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "conversions_qualified",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "cost_per_assist",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "cost_per_conversion",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "ctr",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "date",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "device_type",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "goal",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "goal_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "goal_type",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "impressions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "keyword",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "keyword_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "keyword_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "return_on_ad_spend",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "revenue",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "revenue_per_assist",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "revenue_per_conversion",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "spend",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "view_through_conversions",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "view_through_revenue",
            "mode": "NULLABLE",
            "type": "STRING",
        }
    ],
    dag=dag
)

delete_local_file = PythonOperator(
    task_id='delete_local_file',
    python_callable=remove_local_file,
    dag=dag
)

bing_ads_to_local >> local_data_to_gcs >> delete_old_data >> gcs_data_to_bq >> delete_local_file

if __name__ == "__main__":
    get_bing_ads_data_to_local()
