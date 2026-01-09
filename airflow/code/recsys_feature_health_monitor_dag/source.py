from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURE_HEALTH_MONITOR_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/feature-health-monitor.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
IMAGE_NAME = "feature-health-monitor:latest"

INSTANCE_ID = Variable.get("DEV_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

DOCKER_VOLUME_PATH = Variable.get("DEV_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")

PREFIX = Variable.get("PREFIX")
NEW_PREFIX = Variable.get("NEW_PREFIX")

S3_BUCKET = Variable.get("S3_BUCKET", default_var="vkusvillnew")

AWS_CONN_ID = "s3_experimental"

CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
OPENVPN_CREDENTIALS_PATH = "/etc/openvpn"

LOGIN_GIT = Variable.get("LOGIN_GIT")
TOKEN_GIT = Variable.get("TOKEN_GIT")

# Список конфигов для мониторинга
CONFIG_FILES = [
    "config/config_user.yaml",
    "config/config_raw.yaml",
    "config/config_item.yaml",
    "config/config_query.yaml",
    "config/config_querycat.yaml",
]

with DAG(
        "recsys_feature_health_monitor_dag",
        default_args={
            "depends_on_past": False,
          #  "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
            "owner": "a.velichko",
        },
        description="Feature health monitoring DAG",
        schedule_interval="59 23 * * *",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["recsys", "monitoring", "features", "health"],
) as dag:
    start = DummyOperator(task_id="start")


    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=FEATURE_HEALTH_MONITOR_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_env_file = create_ssh_command_operator(
        task_id="create_env_file",
        command=(
            f"echo 'ENDPOINT_URL={ENDPOINT_URL}\n"
            f"REGION_NAME={REGION_NAME}\n"
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}\n"
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}\n"
            f"S3_BUCKET={S3_BUCKET}' > {repository_name}/.env"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_container_name, start_docker = create_docker_start_operator(
        task_id="start_docker",
        dag_id="{{ dag.dag_id }}",
        repository_name=repository_name,
        volume=[
            f"{CLOUD_DATA_PATH}:/app/data",
            f"{OPENVPN_FOLDER_PATH}:/app/etc/openvpn",
            f"{SSH_FOLDER_PATH}:/root/.ssh",
        ],
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    # Создаем задачи для каждого конфига
    monitor_tasks = []
    for config_file in CONFIG_FILES:
        config_name = config_file.split("/")[-1].replace("config_", "").replace(".yaml", "")
        task_id = f"monitor_{config_name}"

        command = (
            f"docker exec -i {docker_container_name} "
            "python cli.py {{ macros.ds_add(ds, -1) }} "
            f"{config_file}"
        )

        monitor_task = create_ssh_command_operator(
            task_id=task_id,
            command=command,
            ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        )
        monitor_tasks.append(monitor_task)

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    start >> clone_project
    (
            clone_project
            >> create_env_file
            >> start_docker
    )

    # Все задачи мониторинга выполняются параллельно после запуска Docker
    for monitor_task in monitor_tasks:
        start_docker >> monitor_task

    # Все задачи мониторинга должны завершиться перед остановкой Docker
    for monitor_task in monitor_tasks:
        monitor_task >> stop_docker










