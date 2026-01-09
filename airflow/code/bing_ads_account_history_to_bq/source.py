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

last_30_day_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = datetime.today().strftime('%Y-%m-%d')
bq_project = GCPConnectionInfo.gcp_project_test
bq_dataset = 'ads_msft'
bq_table = 'account_history'
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


def get_bing_ads_data_to_local():
    df_data = []
    url = "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13/Accounts/Search"
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DeveloperToken": developer_token,
        'Authorization': f'Bearer {access_token}',
    }

    payload = json.dumps({
        "PageInfo": {
            "Index": 0,
            "Size": 1000
        },
        "Predicates": [
            {
                "Field": "AccountId",
                "Operator": "In",
                "Value": ','.join(get_ad_account_ids())
            }
        ],
        "ReturnAdditionalFields": "AccountMode"
    })

    response = requests.post(
        url=url,
        headers=header,
        data=payload
    )
    response_data = json.loads(response.text)
    if response_data['Accounts']:
        for row in response_data['Accounts']:
            business_address = row.get('BusinessAddress')
            df_row_data = {
                "id": row.get("Id"),
                "last_modified_time": f'{row.get("LastModifiedTime")} UTC',
                "_fivetran_synced": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "account_financial_status": row.get("AccountFinancialStatus"),
                "account_life_cycle_status": row.get("AccountLifeCycleStatus"),
                "auto_tag_type": row.get("AutoTagType"),
                "back_up_payment_instrument_id": row.get("BackUpPaymentInstrumentId"),
                "bill_to_customer_id": row.get("BillToCustomerId"),
                "billing_threshold_amount": row.get("BillingThresholdAmount"),
                "business_address_city": business_address.get("City"),
                "business_address_country_code": business_address.get("CountryCode"),
                "business_address_id": business_address.get("Id"),
                "business_address_line_1": business_address.get("Line1"),
                "business_address_line_2": business_address.get("Line2"),
                "business_address_line_3": business_address.get("Line3"),
                "business_address_line_4": business_address.get("Line4"),
                "business_address_postal_code": business_address.get("PostalCode"),
                "business_address_state_or_province": business_address.get("StateOrProvince"),
                "business_name": business_address.get("BusinessName"),
                "currency_code": row.get("CurrencyCode"),
                "language": row.get("Language"),
                "last_modified_by_user_id": row.get("LastModifiedByUserId"),
                "name": row.get("Name"),
                "number": row.get("Number"),
                "parent_customer_id": row.get("ParentCustomerId"),
                "pause_reason": int(float(row.get("PauseReason"))) if row.get("PauseReason") else None,
                "payment_method_id": row.get("PaymentMethodId"),
                "payment_method_type": row.get("PaymentMethodType"),
                "primary_user_id": row.get("PrimaryUserId"),
                "sales_house_customer_id": row.get("SalesHouseCustomerId"),
                "sold_to_payment_instrument_id": row.get("SoldToPaymentInstrumentId"),
                "time_zone": row.get("TimeZone"),
            }
            df_data.append(df_row_data)
        if not os.path.exists(f'/opt/airflow/files/{bq_dataset}/{bq_table}'):
            os.makedirs(f'/opt/airflow/files/{bq_dataset}/{bq_table}')
        df = pandas.DataFrame(data=df_data, dtype=object)
        df = df.replace(r'\r+|\n+|\t+', '', regex=True)
        df['pause_reason'] = df['pause_reason'].astype('Int64')
        df.to_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv', sep=';', index=False)
        return True
    return False


def remove_old_data():
    list_id = pandas.read_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv',
                              sep=';').id.to_list()
    list_modified_time = pandas.read_csv(f'/opt/airflow/files/{bq_dataset}/{bq_table}/{today_date}.csv',
                                         sep=';').last_modified_time.to_list()
    for i in range(len(list_id)):
        GCPConnectionInfo().execute_query(
            f'DELETE FROM {bq_project}.{bq_dataset}.{bq_table} WHERE id={list_id[i]} and last_modified_time="{list_modified_time[i]}"')

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
    dag_id='bing_ads_account_history_to_bq',
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
            "name": "last_modified_time",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "_fivetran_synced",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
        },
        {
            "name": "account_financial_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "account_life_cycle_status",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "auto_tag_type",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "back_up_payment_instrument_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "bill_to_customer_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "billing_threshold_amount",
            "mode": "NULLABLE",
            "type": "FLOAT",
        },
        {
            "name": "business_address_city",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_country_code",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "business_address_line_1",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_line_2",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_line_3",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_line_4",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_postal_code",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_address_state_or_province",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "business_name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "currency_code",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "language",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "last_modified_by_user_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "number",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "parent_customer_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "pause_reason",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "payment_method_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "payment_method_type",
            "mode": "NULLABLE",
            "type": "STRING",
        },
        {
            "name": "primary_user_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "sales_house_customer_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "sold_to_payment_instrument_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
        },
        {
            "name": "time_zone",
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
