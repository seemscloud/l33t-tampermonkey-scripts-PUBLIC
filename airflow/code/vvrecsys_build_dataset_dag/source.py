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
)
from utils.telegram import create_telegram_alert_func

# Default configurations
GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/vvrecsys.git"
INSTANCE_ID = Variable.get("INSTANCE_ID")

REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
CLOUD_DATA_PATH = "/srv/data"
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_CONN_ID = "s3_experimental"
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = Variable.get("BUCKET_NAME")

default_args = {
    "depends_on_past": False,
    #"on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
    "owner": "a.velichko",
}

with DAG(
    "vvrecsys_build_dataset_dag",
    default_args=default_args,
    start_date=datetime(2024, 7, 9),
    schedule_interval="30 19 * * *",
    catchup=False,
    tags=["vv", "flat", "recsys"],
    params={"l_date": Param(default="", type="string", description="Дата (YYYY-MM-DD)")},
) as dag:
    start = DummyOperator(task_id="start")

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="new_data",
    )

    docker_container_name, start_docker = create_docker_start_operator(
        task_id="start_docker",
        repository_name=repository_name,
        dag_id="{{ dag.dag_id }}",
        volume=f"{CLOUD_DATA_PATH}:/app/data",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    set_date = create_python_in_docker_ssh_command(
        task_id="set_date",
        command="python -m vvrecsys.cli.set_date --config=configs/datasets.yaml --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }}",
        docker_container_name=docker_container_name,
        cmd_timeout=60 * 60,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_datasets = create_python_in_docker_ssh_command(
        task_id="build_datasets",
        command="python -m vvrecsys.cli.datasets --config=configs/datasets.yaml --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }}",
        docker_container_name=docker_container_name,
        cmd_timeout=10 * 60 * 60,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_stop = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    (start >> clone_project >> start_docker >> set_date >> build_datasets >> docker_stop)
