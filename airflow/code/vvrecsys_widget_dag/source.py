from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_python_in_docker_ssh_command,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

# Configuration variables
GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/vvrecsys.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
CLOUD_DATA_PATH = "/srv/data"
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"

# AWS/S3 configuration
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = Variable.get("BUCKET_NAME")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")
DOCKER_VOLUME_PATH = Variable.get("DEV_DOCKER_VOLUME_PATH")

# Git configuration
LOGIN_GIT = Variable.get("LOGIN_GIT")
TOKEN_GIT = Variable.get("TOKEN_GIT")

# S3 paths configuration
PREFIX = Variable.get("PREFIX")
NEW_PREFIX = Variable.get("NEW_PREFIX")
AWS_CONN_ID = "s3_experimental"

MINIO_ENDPOINT_URL = Variable.get("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")

with DAG(
    "vvrecsys_widget_dag",
    default_args={
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "m.gazoian",
    },
    description="DAG for widget recommendations generation",
    schedule_interval="30 08 * * *",
    start_date=datetime(2024, 7, 9),
    catchup=False,
    tags=["vv", "recsys", "widget"],
) as dag:
    start = DummyOperator(task_id="start")

    # Data files to check
    data_folder = "/srv/data/raw"

    parquet_path = "{{ yesterday_ds.replace('-', '/') }}.parquet"

    # Clone repository
    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="python311",  # should be python311
    )

    # Create environment files
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

    create_minio_file = create_ssh_command_operator(
        task_id="create_minio_file",
        command=(
            f"mkdir -p {repository_name}/.credentials;\n"
            f"echo 'endpoint: {MINIO_ENDPOINT_URL}\n"
            f"access_key: {MINIO_ACCESS_KEY}\n"
            f"secret_key: {MINIO_SECRET_KEY}' > {repository_name}/.credentials/minio.yaml"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    # Docker operations
    docker_container_name, start_docker = create_docker_start_operator(
        task_id="start_docker",
        repository_name=repository_name,
        dag_id="{{ dag.dag_id }}",
        volume=[
            f"{CLOUD_DATA_PATH}:/app/data",
            f"{OPENVPN_FOLDER_PATH}:/app/etc/openvpn",
            f"{SSH_FOLDER_PATH}:/root/.ssh",
        ],
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    # Pipeline steps
    set_date = create_python_in_docker_ssh_command(
        task_id="set_date",
        command="python -m vvrecsys.cli.set_date --config=configs/datasets.yaml --date {{ macros.ds_add(ds, -1) }}",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60,  # 1 hour
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_widget = create_python_in_docker_ssh_command(
        task_id="build_widget",
        command="python -m vvrecsys.cli.production --config=configs/production/widget.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=4 * 60 * 60,  # 4 hours
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    deploy_widget0 = create_ssh_command_operator(
        task_id="deploy_widget0",
        command=(
            f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config=configs/production/widget.yaml --index=0 --focus_group=1"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    # Copy results to S3
    copy_files_to_s3 = SSHOperator(
        task_id="copy_results_to_s3",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        command="""
        export AWS_ACCESS_KEY_ID={{ params.aws_access_key }}
        export AWS_SECRET_ACCESS_KEY={{ params.aws_secret_key }}
        aws s3 cp /srv/data/production/widget/ s3://{{ params.bucket_name }}/recsys3/production/{{ macros.ds_add(ds, -1).replace('-', '/') }}/widget/ --recursive --exclude '*' --include '*.h5' --endpoint-url=https://storage.yandexcloud.net
        """,
        params={
            "aws_access_key": AWS_ACCESS_KEY_ID,
            "aws_secret_key": AWS_SECRET_ACCESS_KEY,
            "bucket_name": BUCKET_NAME,
        },
    )

    (
        start
        >> clone_project
        >> create_env_file
        >> create_minio_file
        >> start_docker
        >> set_date
        >> build_widget
        >> deploy_widget0
        >> stop_docker
        >> copy_files_to_s3
    )
