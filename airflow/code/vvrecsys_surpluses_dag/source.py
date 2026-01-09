from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
INSTANCE_ID = Variable.get("INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
OPENVPN_CREDENTIALS_PATH = "/etc/openvpn"
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
MINIO_ENDPOINT_URL = Variable.get("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = Variable.get("BUCKET_NAME")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")
DOCKER_VOLUME_PATH = Variable.get("DEV_DOCKER_VOLUME_PATH")
PREFIX = Variable.get("PREFIX")
NEW_PREFIX = Variable.get("NEW_PREFIX")

LOGIN_GIT = Variable.get("LOGIN_GIT")
TOKEN_GIT = Variable.get("TOKEN_GIT")

AWS_CONN_ID = "s3_experimental"

with DAG(
    "vvrecsys_surpluses_dag",
    default_args={
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "a.velichko",
    },
    description="DAG for empty search",
    schedule_interval="07 * * * *",
    start_date=datetime(2024, 7, 9),
    catchup=False,
    tags=["vv", "test", "emptysearch", "surpluses"],
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=None,
    default_view="grid",
    orientation="LR",
) as dag:
    start = DummyOperator(
        task_id="start",
    )
    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="feature/surpluses",
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

    set_date = create_python_in_docker_ssh_command(
        task_id="set_date",
        command="python -m vvrecsys.cli.set_date  --config=configs/datasets.yaml --date {{ macros.ds_add(ds, -1) }}",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 1,  # 1 hour
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_emptysearch_surpluses = create_python_in_docker_ssh_command(
        task_id="build_emptysearch",
        command="python -m vvrecsys.cli.production --config=configs/production/emptysearchsurpluses.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    deploy_surpluses = create_ssh_command_operator(
        task_id="deploy_surpluses",
        command=(
            f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config=configs/production/emptysearchsurpluses.yaml --index=0 --focus_group=1"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    loop_trigger = TriggerDagRunOperator(
        task_id="loop_trigger",
        trigger_dag_id="{{ dag.dag_id }}",  # Same DAG ID
        wait_for_completion=False,
    )

    # Define Task Flow
    start >> clone_project >> create_env_file >> create_minio_file >> start_docker
    start_docker >> set_date >> build_emptysearch_surpluses >> deploy_surpluses >> stop_docker >> loop_trigger
