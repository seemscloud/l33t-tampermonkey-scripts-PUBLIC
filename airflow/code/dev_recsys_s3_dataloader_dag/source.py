from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

from operators.compute_cloud_operator import start_compute_cloud_operator
from utils.telegram import create_telegram_alert_func
from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_ssh_command_operator,
)
GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
DATALOADER_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys_dataloader.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_dev_connection"

INSTANCE_ID = Variable.get("DEV_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

LOGIN_GIT = Variable.get("LOGIN_GIT")
TOKEN_GIT = Variable.get("TOKEN_GIT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = Variable.get("BUCKET_NAME")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")
DOCKER_VOLUME_PATH = Variable.get("DEV_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
PREFIX = Variable.get("PREFIX")
NEW_PREFIX = Variable.get("NEW_PREFIX")
CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"

with DAG(
    "dev_recsys_s3_dataloader_dag",
    default_args={
        "depends_on_past": False,
        "retries": 1,
       "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "retry_delay": timedelta(minutes=5),
        "owner": "v.zelenin",
    },
    description="DAG for s3 data loader",
    schedule_interval="30 17 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["recsys", "s3", "dataloader", "dev"],
) as dag:
    start = DummyOperator(task_id="start")

    start_compute_cloud = start_compute_cloud_operator(
        task_id="start_compute_cloud",
        instance_id=INSTANCE_ID,
        authorized_keys=AUTHORIZED_KEYS,
    )

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=DATALOADER_REPO,
        dag_id = "{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_env_file = create_ssh_command_operator(
        task_id="create_env_file",
        command=(
            f"echo 'LOGIN_GIT={LOGIN_GIT}\n"
            f"BUCKET_NAME={BUCKET_NAME}\n"
            f"ENDPOINT_URL={ENDPOINT_URL}\n"
            f"REGION_NAME={REGION_NAME}\n"
            f"PREFIX={PREFIX}\n"
            f"NEW_PREFIX={NEW_PREFIX}\n"
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}\n"
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}\n"
            f"TOKEN_GIT={TOKEN_GIT}' > {repository_name}/.env"
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

    fetch_data = create_ssh_command_operator(
        task_id="fetch_data",
        command=f"docker exec -i {docker_container_name} python -m src.data_loader s3_pull --config_path dataloader.yaml",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    (
        start
        >> start_compute_cloud
        >> clone_project
        >> create_env_file
        >> start_docker
        >> fetch_data
        >> stop_docker
    )
