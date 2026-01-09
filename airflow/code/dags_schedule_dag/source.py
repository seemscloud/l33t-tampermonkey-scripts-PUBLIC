import io
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session
from airflow.utils import timezone

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


def compute_and_upload_runtimes(**context):
    days = 15
    s3 = boto3_session()

    with Session() as session:
        runs = (
            session.query(DagRun)
            .filter(
                DagRun.state == "success",
                DagRun.start_date >= timezone.utcnow() - timedelta(days=days),
            )
            .all()
        )

    rows = []
    for r in runs:
        if r.start_date and r.end_date:
            delta = r.end_date - r.start_date
            duration_minutes = int(delta.total_seconds() // 60)
            hours, minutes = divmod(duration_minutes, 60)

            rows.append(
                {
                    "dag_id": r.dag_id,
                    "execution_date": r.execution_date.strftime("%Y-%m-%d %H:%M"),
                    "start_time": r.start_date.strftime("%H:%M"),
                    "end_time": r.end_date.strftime("%H:%M"),
                    "duration_minutes": duration_minutes,
                    "duration": f"{hours}h {minutes}m",
                }
            )

    if not rows:
        print("There are no data for", days, "days")
        return

    df = pd.DataFrame(rows)

    avg_df = (
        df.groupby("dag_id")["duration_minutes"]
        .mean()
        .reset_index()
        .rename(columns={"duration_minutes": "avg_minutes"})
    )
    avg_df["avg_duration"] = avg_df["avg_minutes"].apply(lambda x: f"{int(x // 60)}h {int(x % 60)}m")

    df = df.merge(avg_df[["dag_id", "avg_duration"]], on="dag_id", how="left")
    df = df.drop(columns=["duration_minutes"])

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    key = "recsys3/dags_metrics/dag_runtime.csv"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())

    print(f"Saved: s3://{BUCKET_NAME}/{key}")


with DAG(
    "dags_schedule_dag",
    default_args={
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "m.gazoian",
    },
    description="Compute DAG runtimes",
    schedule_interval="40 23 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dags", "metrics", "prod"],
) as dag:
    PythonOperator(
        task_id="compute_and_upload_runtimes",
        python_callable=compute_and_upload_runtimes,
        provide_context=True,
    )
