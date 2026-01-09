from collections import defaultdict
from datetime import datetime
from pathlib import PurePosixPath

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils.telegram import create_telegram_alert_func

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = Variable.get("BUCKET_NAME")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")


def boto3_session():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=ENDPOINT_URL,
    )


def cleanup_old_artifacts(base_prefix: str, keep_last: int = 10):
    s3 = boto3_session()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=base_prefix)

    grouped_by_object = defaultdict(lambda: defaultdict(list))

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = PurePosixPath(key).parts
            try:
                if len(parts) < 6:
                    continue
                year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
                object_key = parts[5]
                date = datetime(year, month, day)
                grouped_by_object[object_key][date].append(key)
            except Exception:
                continue

    folders_to_check = set()

    for object_key, dated_keys in grouped_by_object.items():
        sorted_dates = sorted(dated_keys.keys(), reverse=True)
        dates_to_delete = sorted_dates[keep_last:]

        print(f"[{object_key}] Total dates: {len(sorted_dates)} → Deleting: {len(dates_to_delete)}")

        for date in dates_to_delete:
            keys = dated_keys[date]
            for key in keys:
                try:
                    s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                    print(f"  Deleted: {key}")
                except Exception as e:
                    print(f"  Failed to delete {key}: {e}")
            try:
                base_path = PurePosixPath(keys[0]).parents[0]
                folders_to_check.add(str(base_path))
            except Exception:
                continue

    for folder in folders_to_check:
        paginator = s3.get_paginator("list_objects_v2")
        result = paginator.paginate(Bucket=BUCKET_NAME, Prefix=folder + "/")
        has_content = any("Contents" in page for page in result)
        if not has_content:
            try:
                s3.delete_object(Bucket=BUCKET_NAME, Key=folder + "/")
                print(f"  Removed empty folder: {folder}")
            except Exception as e:
                print(f"  Failed to delete empty folder {folder}: {e}")

        try:
            parts = PurePosixPath(folder).parts
            if len(parts) >= 4:
                day_prefix = "/".join(parts[:5])
                month_prefix = "/".join(parts[:4])
                year_prefix = "/".join(parts[:3])

                for prefix in [day_prefix, month_prefix, year_prefix]:
                    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix + "/")
                    if result.get("KeyCount", 0) == 0:
                        s3.delete_object(Bucket=BUCKET_NAME, Key=prefix + "/")
                        print(f"  Removed empty parent folder: {prefix}")
        except Exception:
            continue


with DAG(
    "vvrecsys_artifacts_cleanup_dag",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "m.gazoian",
    },
    description="DAG for deleting old artifacts on s3",
    schedule_interval="30 19 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "cleanup", "prod"],
) as dag:
    start = DummyOperator(task_id="start")

    cleanup_shelves = PythonOperator(
        task_id="cleanup_shelves",
        python_callable=cleanup_old_artifacts,
        op_kwargs={
            "base_prefix": "recsys3/production",
            "keep_last": 8,
        },
    )

    cleanup_models = PythonOperator(
        task_id="cleanup_models",
        python_callable=cleanup_old_artifacts,
        op_kwargs={
            "base_prefix": "recsys3/models",
            "keep_last": 8,
        },
    )

    start >> cleanup_shelves >> cleanup_models
