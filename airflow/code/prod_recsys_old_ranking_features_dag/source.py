from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from operators.remote_operator import (
    RemoteFileSensor,
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys_ranking_features.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
IMAGE_NAME = "recsys_features_ranking:latest"

INSTANCE_ID = Variable.get("PROD_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_CONN_ID = "s3_experimental"
BUCKET_NAME = Variable.get("BUCKET_NAME")
PARENT_DAG = "prod_recsys_s3_dataloader"

CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
OPENVPN_CREDENTIALS_PATH = "/etc/openvpn"

with DAG(
    "prod_recsys_old_ranking_features_dag",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "a.velichko",
    },
    description="Ranking features calculation DAG",
    schedule_interval="30 18 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "features", "prod"],
) as dag:
    start = DummyOperator(task_id="start")

    data_folder = "/srv/data/raw"
    folder_names_at_remote = [
        "baskets",
        "clicks",
        "sales",
    ]
    parquet_path = "{{ ds.replace('-', '/') }}.parquet"

    check_file_sensors = []
    for folder in folder_names_at_remote:
        check_file_sensor = RemoteFileSensor(
            task_id=f"check_remote_file_in_{folder}",
            ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            remote_filepath=f"{data_folder}/{folder}/{parquet_path}",
            timeout=24 * 60 * 60,
            poke_interval=30 * 60,
            mode="reschedule",
        )
        check_file_sensors.append(check_file_sensor)

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=FEATURES_PIPELINE_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="python311-old-features",
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

    create_item_features = create_ssh_command_operator(
        task_id="create_item_features",
        command=f"docker exec -i {docker_container_name} "
        f"python -m cli create_features "
        f"--config_path=src/config/config_docker.yaml "
        f"--dataset_type=item "
        f"--first_date={{{{ ds }}}}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_useritem_features = create_ssh_command_operator(
        task_id="create_useritem_features",
        command=f"docker exec -i {docker_container_name} "
        f"python -m cli create_features  "
        f"--config_path=src/config/config_docker.yaml "
        f"--dataset_type=useritem "
        f"--first_date={{{{ ds }}}}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_usercat_features = create_ssh_command_operator(
        task_id="create_usercat_features",
        command=f"docker exec -i {docker_container_name} "
        f"python -m cli create_features "
        f"--config_path=src/config/config_docker.yaml "
        f"--dataset_type=usercat "
        f"--first_date={{{{ ds }}}}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    conn = BaseHook.get_connection(AWS_CONN_ID)
    aws_access_key = conn.login
    aws_secret_key = conn.password
    rerank_features_folder = "/srv/data/rerank_features"
    s3_folder = "recsys3/rerank_features"
    features_folder_names = ["item", "usercat", "useritem"]

    copy_operators = []
    for folder in features_folder_names:
        copy_files_to_s3 = SSHOperator(
            task_id=f"copy_files_to_s3_from_{folder}",
            ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            command=f"""
            export AWS_ACCESS_KEY_ID={{{{ params.aws_access_key }}}}
            export AWS_SECRET_ACCESS_KEY={{{{ params.aws_secret_key }}}}
            aws s3 cp {rerank_features_folder}/{folder}/{{{{ ds.replace('-', '/') }}}}.parquet s3://{{{{ params.bucket_name }}}}/{s3_folder}/{folder}/{{{{ ds.replace('-', '/') }}}}.parquet --endpoint-url=https://storage.yandexcloud.net
            """,
            params={
                "aws_access_key": aws_access_key,
                "aws_secret_key": aws_secret_key,
                "bucket_name": BUCKET_NAME,
            },
        )
        copy_operators.append(copy_files_to_s3)

    for sensor in check_file_sensors:
        start >> sensor
        sensor >> clone_project

    (
        clone_project
        >> start_docker
        >> create_item_features
        >> create_useritem_features
        >> create_usercat_features
        >> stop_docker
    )

    for copy_operator in copy_operators:
        stop_docker >> copy_operator
