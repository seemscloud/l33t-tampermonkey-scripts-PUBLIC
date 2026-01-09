from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

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
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_dev_connection"
IMAGE_NAME = "recsys_ranking_features:latest"

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

AWS_CONN_ID = "s3_experimental"


CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
OPENVPN_CREDENTIALS_PATH = "/etc/openvpn"


LOGIN_GIT = Variable.get("LOGIN_GIT")
TOKEN_GIT = Variable.get("TOKEN_GIT")
with DAG(
    "recsys_new_ranking_features_dag",
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
        "feedbacks",
        "query_search",
        "raitings",
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
    )

    create_env_file = create_ssh_command_operator(
        task_id="create_env_file",
        command=(
            f"echo 'ENDPOINT_URL={ENDPOINT_URL}\n"
            f"REGION_NAME={REGION_NAME}\n"
            f"GITLAB_TOKEN={GITLAB_TOKEN_VV}\n"
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}\n"
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}' > {repository_name}/.env"
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

    collect_dataframes = create_ssh_command_operator(
        task_id="collect_dataframes",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --command=collect",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_item_features = create_ssh_command_operator(
        task_id="create_item_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=item",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_useritem_features = create_ssh_command_operator(
        task_id="create_useritem_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=useritem",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_usercat_features = create_ssh_command_operator(
        task_id="create_usercat_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=usercat",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_query_features = create_ssh_command_operator(
        task_id="create_query_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=query",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_queryitem_features = create_ssh_command_operator(
        task_id="create_queryitem_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=queryitem",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_querycat_features = create_ssh_command_operator(
        task_id="create_querycat_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=querycat",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )


    create_user_features = create_ssh_command_operator(
        task_id="create_user_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --group=user",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    delete_collection_temporary_dataframe = create_ssh_command_operator(
        task_id="delete_collection_temporary_dataframe",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=config/config_new.yaml --date {{{{ ds }}}} --command=delete_collect",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    for sensor in check_file_sensors:
        start >> sensor
        sensor >> clone_project

    (
        clone_project
        >> create_env_file
        >> start_docker
        >> collect_dataframes
        >> create_useritem_features
        >> create_item_features
        >> create_usercat_features
        >> create_query_features
        >> create_queryitem_features
        >> create_querycat_features
        >> create_user_features
        >> delete_collection_temporary_dataframe
        >> stop_docker
    )
