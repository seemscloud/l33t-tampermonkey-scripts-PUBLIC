from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_python_in_docker_ssh_command,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/vvrecsys.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"

DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")

with DAG(
    "vvrecsys_ratings_dag",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "v.pandov",
    },
    description="DAG for training and inference models",
    schedule_interval="30 08 * * *",
    start_date=datetime(2024, 7, 9),
    catchup=False,
    tags=["vv", "recsys"],
) as dag:
    start = DummyOperator(
        task_id="start",
    )

    # data_folder = "/srv/data/raw"
    # folder_names_at_remote = [
    #     "feedbacks",
    #     "raitings",
    # ]
    # parquet_path = "{{ yesterday_ds.replace('-', '/') }}.parquet"

    # check_file_sensors = []
    # for folder in folder_names_at_remote:
    #     check_file_sensor = RemoteFileSensor(
    #         task_id=f"check_remote_file_in_{folder}",
    #         ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    #         remote_filepath=f"{data_folder}/{folder}/{parquet_path}",
    #         timeout=24 * 60 * 60,
    #         poke_interval=30 * 60,
    #         mode="reschedule",
    #     )
    #     check_file_sensors.append(check_file_sensor)

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_project",
        repository_url=GITLAB_REPO,
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
        dag_id="{{ dag.dag_id }}",
        repository_name=repository_name,
        volume=f"{DOCKER_VOLUME_PATH}:/app/data",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_ratings = create_python_in_docker_ssh_command(
        task_id="build_ratings",
        command="python -m vvrecsys.cli.ratings --config=configs/ratings.yaml --start_date={{ macros.ds_add(ds, -360) }} --end_date={{ ds }}",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 1,  # 1 hour
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    start >> clone_project >> create_env_file >> start_docker >> build_ratings >> stop_docker
