from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

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

GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys-metrics-validator.git"
INSTANCE_ID = Variable.get("INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
CLOUD_DATA_PATH = "/srv/data"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

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

validation_configs = [
    ("historytape", "configs/historytape_validate.yaml"),
    ("lp", "configs/lp_validate.yaml"),
    ("emptysearch_tifu_cleanout_ranker", "configs/emptysearch_tifu_cleanout_ranker_validate.yaml"),
    ("oftenbuy_class_item", "configs/oftenbuy_class_item_validate.yaml"),
    ("vkusback", "configs/vkusback_validate.yaml"),
    ("promo", "configs/promo_validate.yaml"),
    ("abonement_anticleanout", "configs/abonement_anticleanout_validate.yaml"),
    ("abonement_cleanout", "configs/abonement_cleanout_validate.yaml"),
    ("abonement_noveltis", "configs/abonement_noveltis_validate.yaml"),
    ("widget", "configs/widget_validatet.yaml"),
    ("rp", "configs/rp_validate.yaml"),
    ("add_to_cart", "configs/add_to_cart_validate.yaml"),
    ("add_to_cart_ranking", "configs/add_to_cart_ranking_validate.yaml"),
    ("empty_search_card", "configs/empty_search_card_validate.yaml"),
    ("empty_search_cart", "configs/empty_search_cart_validate.yaml"),
    ("empty_search_catalog", "configs/empty_search_catalog_validate.yaml"),
    ("oftenbuy_new_ranking", "configs/oftenbuy_new_ranking.yaml"),
    ("oftenbuy_new_ranking_margin_boosting", "configs/oftenbuy_new_ranking_margin_boosting.yaml"),
    ("personalsearch", "configs/personalsearch_validate.yaml"),
]


with DAG(
    "vvrecsys_metrics_validator_dag",
    default_args={
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "v.zelenin",
        # "end_date": datetime(2024, 11, 29),
    },
    description="DAG lp",
    schedule_interval="05 18 * * *",
    start_date=datetime(2024, 7, 9),
    catchup=False,
    tags=["vv", "recsys", "metrics_validator"],
) as dag:
    start = DummyOperator(
        task_id="start",
    )

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
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
        dag_id="{{ dag.dag_id }}",
        repository_name=repository_name,
        volume=[f"{CLOUD_DATA_PATH}:/app/data", f"{SSH_FOLDER_PATH}:/root/.ssh"],
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    with TaskGroup("calculate_metrics_group") as calculate_group:
        previous_task = None

        for name, config_path in validation_configs:
            task = create_python_in_docker_ssh_command(
                task_id=f"calculate_metrics_{name}",
                command=f"python -m cli.run_calculate_metrics {{{{ macros.ds_add(ds, -1) }}}} --config_path {config_path}",
                docker_container_name=docker_container_name,
                cmd_timeout=1 * 60 * 60 * 1,  # 1 hour
                ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            )
            task.trigger_rule = TriggerRule.ALL_DONE

            if previous_task:
                previous_task.set_downstream(task)

            previous_task = task

    with TaskGroup("compare_metrics_group") as compare_group:
        compare_tasks = []
        for name, config_path in validation_configs:
            task = create_python_in_docker_ssh_command(
                task_id=f"compare_metrics_{name}",
                command=f"python -m cli.run_compare_metrics {{{{ macros.ds_add(ds, -1) }}}} --config_path {config_path}",
                docker_container_name=docker_container_name,
                cmd_timeout=1 * 60 * 60 * 1,  # 1 hour
                ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
            )
            task.trigger_rule = TriggerRule.ALL_DONE
            compare_tasks.append(task)

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    (start >> clone_project >> create_env_file >> start_docker >> calculate_group >> compare_group >> stop_docker)
