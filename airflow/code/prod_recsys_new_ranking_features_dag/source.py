from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import (
    RemoteFileSensor,
    create_clone_repository_operator,
    create_docker_stop_operator,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys_ranking_features.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
IMAGE_NAME = "recsys_ranking_features:latest"

INSTANCE_ID = Variable.get("PROD_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")

PREFIX = Variable.get("PREFIX")
NEW_PREFIX = Variable.get("NEW_PREFIX")

AWS_CONN_ID = "s3_experimental"

PARENT_DAG = "prod_recsys_s3_dataloader"

with DAG(
    "prod_recsys_new_ranking_features_dag",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "a.velichko",
    },
    description="Ranking features calculation DAG",
    schedule_interval="00 19 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "features", "prod"],
) as dag:
    start = DummyOperator(task_id="start")

    data_folder = "/srv/data/raw"
    folder_names_at_remote = ["baskets", "clicks", "sales", "feedbacks"]
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
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}\n"
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}' > {repository_name}/.env"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_docker_image = create_ssh_command_operator(
        task_id="build_image",
        command=f"docker build --no-cache --rm -t {IMAGE_NAME} {repository_name}/.",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_container_name = "recsys_ranking_features_run_container"

    run_docker_container = create_ssh_command_operator(
        task_id="run_docker_container",
        command=f"docker stop {docker_container_name};"
        f" docker rm {docker_container_name};"
        f" docker run --name {docker_container_name} -it -d -v '{DOCKER_VOLUME_PATH}:/{DOCKER_VOLUME_PATH}' {IMAGE_NAME}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    collect_dataframes = create_ssh_command_operator(
        task_id="collect_dataframes",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=src/config/config_new.yaml --date {{{{ ds }}}} --command=collect",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_item_features = create_ssh_command_operator(
        task_id="create_item_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=src/config/config_new.yaml --date {{{{ ds }}}} --group=item",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_useritem_features = create_ssh_command_operator(
        task_id="create_useritem_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=src/config/config_new.yaml --date {{{{ ds }}}} --group=useritem",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    create_usercat_features = create_ssh_command_operator(
        task_id="create_usercat_features",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=src/config/config_new.yaml --date {{{{ ds }}}} --group=usercat",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    delete_collection_temporary_dataframe = create_ssh_command_operator(
        task_id="delete_collection_temporary_dataframe",
        command=f"docker exec -i {docker_container_name} python -m cli  --config=src/config/config_new.yaml --date {{{{ ds }}}} --command=delete_collect",
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
        >> build_docker_image
        >> run_docker_container
        >> collect_dataframes
        >> create_item_features
        >> create_useritem_features
        >> create_usercat_features
        >> delete_collection_temporary_dataframe
        >> stop_docker
    )
