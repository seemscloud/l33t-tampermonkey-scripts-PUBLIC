from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_ssh_command_operator,
)

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/vvrecsys.git"

REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection_backup"
CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

DEV_MINIO_ENDPOINT_URL = Variable.get("DEV_MINIO_ENDPOINT_URL")
DEV_MINIO_ACCESS_KEY = Variable.get("DEV_MINIO_ACCESS_KEY")
DEV_MINIO_SECRET_KEY = Variable.get("DEV_MINIO_SECRET_KEY")
DEV_MINIO_REGION_NAME = Variable.get("DEV_MINIO_REGION_NAME")

CONFIGS_TO_DEPLOY = [
    # {"config": "empty_search_card"},
    # {"config": "empty_search_cart"},
    # {"config": "coitems", "params": "--coitems"},
    # {"config": "emptysearch", "params": "--index=0 --focus_group=2"},
    # {"config": "emptysearch_novelty_light", "params": "--index=0 --focus_group=3"},
    # {"config": "emptysearch_novelty_moderate", "params": "--index=0 --focus_group=4"},
    # {"config": "emptysearch_novelty_aggressive", "params": "--index=0 --focus_group=5"},
    # {"config": "add_to_cart", "params": "--index=0 --focus_group=1"},
    # {"config": "add_to_cart", "params": "--index=1 --focus_group=2"},
    {"config": "oftenbuy", "params": "--index=0 --focus_group=1"},
    {"config": "oftenbuy", "params": "--index=1 --focus_group=2"},
    {"config": "oftenbuy", "params": "--index=2 --focus_group=3"},
    # {"config": "promo"},
    # {"config": "historytape"},
    {"config": "widget"},
    {"config": "empty_search_catalog"},
]

with DAG(
    "vvrecsys_upload_minio",
    default_args={
        # "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "vzelenin",
    },
    description="DAG для загрузки артефактов в minio",
    schedule_interval=None,
    start_date=datetime(2025, 10, 23),
    catchup=False,
    tags=["vv", "recsys", "upload", "minio"],
) as dag:
    start = DummyOperator(task_id="start")

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="upload_test_minio",
    )

    create_minio_file = create_ssh_command_operator(
        task_id="create_minio_file",
        command=(
            f"mkdir -p {repository_name}/.credentials;\n"
            f"echo 'endpoint: {DEV_MINIO_ENDPOINT_URL}\n"
            f"access_key: {DEV_MINIO_ACCESS_KEY}\n"
            f"secret_key: {DEV_MINIO_SECRET_KEY}\n"
            f"region: {DEV_MINIO_REGION_NAME}' > {repository_name}/.credentials/minio.yaml"
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
        cpus=2,
    )

    upload_to_minio_tasks = []
    for idx, config_item in enumerate(CONFIGS_TO_DEPLOY):
        config_name = config_item["config"]
        extra_params = config_item.get("params", "")

        command = (
            f"docker exec -i {docker_container_name} "
            f"python -m vvrecsys.cli.minio --config=configs/production/{config_name}.yaml"
        )
        if extra_params:
            command += f" {extra_params}"

        task_id = f"upload_to_minio_{idx}_{config_name.replace('-', '_')}"

        upload_to_minio_task = create_ssh_command_operator(
            task_id=task_id,
            command=command,
            ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            trigger_rule="all_done",
        )
        upload_to_minio_tasks.append(upload_to_minio_task)

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    stop_docker.trigger_rule = "all_done"

    start >> clone_project >> create_minio_file >> start_docker

    prev_task = start_docker
    for upload_to_minio_task in upload_to_minio_tasks:
        prev_task >> upload_to_minio_task
        prev_task = upload_to_minio_task

    prev_task >> stop_docker
