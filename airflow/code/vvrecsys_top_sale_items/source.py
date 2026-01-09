from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import (
    RemoteFileSensor,
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_python_in_docker_ssh_command,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys-ad-hoc.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"

TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")

CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"

with DAG(
    "vvrecsys_top_sale_items",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "v.zelenin",
    },
    description="DAG for top_sale_items",
    schedule_interval="30 08 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "top_sale_items", "prod"],
) as dag:
    start = DummyOperator(task_id="start")

    data_folder = "/srv/data/raw"
    sales_folder = "sales"
    parquet_path = "{{ yesterday_ds.replace('-', '/') }}.parquet"

    check_file_sensor = RemoteFileSensor(
        task_id=f"check_remote_file_in_{sales_folder}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        remote_filepath=f"{data_folder}/{sales_folder}/{parquet_path}",
        timeout=24 * 60 * 60,
        poke_interval=30 * 60,
        mode="reschedule",
    )

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=FEATURES_PIPELINE_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_env_file = create_ssh_command_operator(
        task_id="create_env_file",
        command=(
            f"echo 'ENDPOINT_URL={ENDPOINT_URL}\n"
            f"REGION_NAME={REGION_NAME}\n"
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}\n"
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}' > {repository_name}/.env"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_container_name, start_docker = create_docker_start_operator(
        task_id="start_docker",
        repository_name=repository_name,
        dag_id="{{ dag.dag_id }}",
        volume=f"{CLOUD_DATA_PATH}:/app/data",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_top_sale_items = create_python_in_docker_ssh_command(
        task_id="build_top_sale_items",
        command="python -m src.cli.run --script=top_sale_items  --start_date={{ macros.ds_add(ds, -16) }} --end_date={{ macros.ds_add(ds, -1) }} --config=./configs/top_sale_items.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    (
        start
        >> check_file_sensor
        >> clone_project
        >> create_env_file
        >> start_docker
        >> build_top_sale_items
        >> stop_docker
    )
