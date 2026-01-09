from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

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


model_configs = [
    {
        "model_name": "oftenbuy",
        "train_task": {
            "task_id": "train_oftenbuy",
            "command": "python -m vvrecsys.cli.training --config=configs/models/oftenbuy.yaml",
            "timeout": 1 * 60 * 60 * 10,  # 10 minutes
        },
        "predict_task": {
            "task_id": "predict_oftenbuy",
            "command": "python -m vvrecsys.cli.inference --config=configs/models/oftenbuy.yaml --K=1000 --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }} --force",
            "timeout": 1 * 60 * 60 * 3,  # 3 hours
        },
    },
    {
        "model_name": "implicit_tfidf_6m_k200",
        "train_task": {
            "task_id": "train_implicit_tfidf_6m_k200",
            "command": "python -m vvrecsys.cli.training --config=configs/models/implicit_tfidf_6m_k200.yaml",
            "timeout": 1 * 60 * 60 * 10,
        },
        "predict_task": {
            "task_id": "predict_implicit_tfidf_6m_k200",
            "command": "python -m vvrecsys.cli.inference --config=configs/models/implicit_tfidf_6m_k200.yaml --K=500 --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }} --force",
            "timeout": 1 * 60 * 60 * 10,
        },
    },
    {
        "model_name": "iptop",
        "train_task": {
            "task_id": "train_iptop",
            "command": "python -m vvrecsys.cli.training --config=configs/models/iptop.yaml",
            "timeout": 1 * 60 * 60 * 10,
        },
        "predict_task": {
            "task_id": "predict_iptop",
            "command": "python -m vvrecsys.cli.inference --config=configs/models/iptop.yaml --K=1000 --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }} --force",
            "timeout": 1 * 60 * 60 * 10,
        },
    },
    {
        "model_name": "usercategory",
        "train_task": {
            "task_id": "train_usercategory",
            "command": "python -m vvrecsys.cli.training --config=configs/models/usercategory.yaml",
            "timeout": 1 * 60 * 60 * 10,
        },
        "predict_task": {
            "task_id": "predict_usercategory",
            "command": "python -m vvrecsys.cli.inference --config=configs/models/usercategory.yaml --K=50 --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }} --force",
            "timeout": 1 * 60 * 60 * 10,
        },
    },
]

default_args = {
    "depends_on_past": False,
    "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
    "owner": "s.nikiforov",
}

with DAG(
    "vvrecsys_train_and_inference_parent_candidate_dag_part2",
    default_args=default_args,
    start_date=datetime(2024, 7, 9),
    schedule_interval="40 00 * * *",
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
        branch_name="python311",
    )

    dag_id = "{{ dag.dag_id }}"
    docker_container_name, start_docker = create_docker_start_operator(
        task_id="start_docker",
        repository_name=repository_name,
        dag_id=dag_id,
        volume=f"{CLOUD_DATA_PATH}:/app/data",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        cpus=16,
    )

    set_date = create_python_in_docker_ssh_command(
        task_id="set_date",
        command="python -m vvrecsys.cli.set_date --config=configs/datasets.yaml --date {{ params.l_date if params.l_date else macros.ds_add(data_interval_end | ds, -1) }}",
        docker_container_name=docker_container_name,
        cmd_timeout=60 * 60,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    start >> clone_project >> start_docker >> set_date

    previous_group = None

    for model in model_configs:
        model_name = model["model_name"]

        with TaskGroup(group_id=f"{model_name}_pipeline") as model_group:
            train = create_python_in_docker_ssh_command(
                task_id=f"train_{model_name}",
                command=model["train_task"]["command"],
                docker_container_name=docker_container_name,
                cmd_timeout=model["train_task"]["timeout"],
                ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            )

            predict = create_python_in_docker_ssh_command(
                task_id=f"predict_{model_name}",
                command=model["predict_task"]["command"],
                docker_container_name=docker_container_name,
                cmd_timeout=model["predict_task"]["timeout"],
                ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            )

            copy_files_to_s3 = SSHOperator(
                task_id=f"upload_candidates_{model_name}",
                ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
                command="""
                export AWS_ACCESS_KEY_ID={{ params.aws_access_key }}
                export AWS_SECRET_ACCESS_KEY={{ params.aws_secret_key }}
                aws s3 cp /srv/data/models/{{ params.model_name }}/candidates \
                s3://{{ params.bucket_name }}/recsys3/models/{{ (params.l_date if params.l_date else ds) | replace('-', '/') }}/{{ params.model_name }}/ \
                --recursive --exclude '*' --include '*.parquet' --endpoint-url=https://storage.yandexcloud.net
                """,
                params={
                    "aws_access_key": AWS_ACCESS_KEY_ID,
                    "aws_secret_key": AWS_SECRET_ACCESS_KEY,
                    "bucket_name": BUCKET_NAME,
                    "model_name": model_name,
                },
            )

            model_done = DummyOperator(
                task_id=f"{model_name}_done",
                trigger_rule=TriggerRule.ALL_DONE,
            )

            train >> predict >> copy_files_to_s3 >> model_done

        set_date >> model_group

        if previous_group:
            previous_group >> model_group

        previous_group = model_group

    docker_stop = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    docker_stop.trigger_rule = TriggerRule.ALL_DONE

    previous_group >> docker_stop
