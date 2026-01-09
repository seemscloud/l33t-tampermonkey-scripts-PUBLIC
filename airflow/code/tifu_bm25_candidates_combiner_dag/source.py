from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_python_in_docker_ssh_command,
    create_ssh_command_operator,
)

# Load Variables
GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys-ad-hoc.git"
AWS_CONN_ID = "s3_experimental"
BUCKET_NAME = Variable.get("BUCKET_NAME")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
REGION_NAME = Variable.get("REGION_NAME")
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
# Optional config path in Docker container
CONFIG_PATH_IN_CONTAINER = "./configs/user_store_map.yaml"

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")

CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"

with DAG(
    dag_id="tifu_bm25_candidates_combiner_dag",
    default_args={
        "depends_on_past": False,
        #  "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "a.velichko",
    },
    description="combine tifu and bm25 candidates",
    schedule_interval="0 22 * * *",  # Daily at 4 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["recsys", "candidates"],
    params={
        "logic_date": Param(default="", type="string", description="Дата запуска (для сенсоров)"),
    },
) as dag:
    start = DummyOperator(task_id="start")
    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="feature/candidates-mixter",
    )

    create_env_file = create_ssh_command_operator(
        task_id="create_env_file",
        command=(
            f"echo 'ENDPOINT_URL={ENDPOINT_URL}\n"
            f"REGION_NAME={REGION_NAME}\n"
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}\n"
            f"MINIO_ACCESS_KEY={MINIO_ACCESS_KEY}\n"
            f"MINIO_SECRET_KEY={MINIO_SECRET_KEY}\n"
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

    combine_candidates = create_python_in_docker_ssh_command(
        task_id="build_user_store_map",
        command="python -m src.cli.combine_candidates --tifu-path data/models/iptop/candidates   --bm25-path data/models/bm25/candidates   --output-path data/models/tifu_with_bm25/candidates",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    end = DummyOperator(task_id="end")

    (start >> clone_project >> create_env_file >> start_docker >> combine_candidates >> stop_docker >> end)
