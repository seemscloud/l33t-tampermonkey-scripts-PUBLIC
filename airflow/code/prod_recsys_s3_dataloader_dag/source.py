from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models.param import Param

from operators.remote_operator import create_ssh_command_operator, create_clone_repository_operator
from utils.telegram import create_telegram_alert_func

INSTANCE_ID = Variable.get("PROD_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
DATALOADER_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys_dataloader.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
IMAGE_NAME = "recsys_s3_dataloader:latest"
CONTAINER_VOLUME_PATH = "/app/data"

LOGIN_GIT = Variable.get("LOGIN_GIT")
TOKEN_GIT = Variable.get("TOKEN_GIT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_CONN_ID = "s3_experimental"
BUCKET_NAME = Variable.get("BUCKET_NAME")
ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")
DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
PREFIX = Variable.get("PREFIX")
NEW_PREFIX = Variable.get("NEW_PREFIX")

with DAG(
    "prod_recsys_s3_dataloader_dag",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "retry_delay": timedelta(minutes=5),
        "owner": "s.nikiforov",
    },
    description="DAG for s3 data loader",
    schedule_interval="30 17 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["recsys", "s3", "dataloader", "prod"],
    params={
        "logic_date": Param(default="", type="string", description="Дата запуска (для сенсоров)"),
    }
) as dag:
    start = DummyOperator(task_id="start")

    folder_names = [
        "recsys3/output_data/baskets",
        "recsys3/output_data/clicks",
        "recsys3/output_data/sales",
    ]
    sensors = []

    for folder in folder_names:
        s3_key = f"{folder}/{{{{ (params.logic_date if params.logic_date else ds) | replace('-', '/') }}}}.parquet"
        folder_truncated = folder.split("/")[-1]

        sensor = S3KeySensor(
            task_id=f"wait_for_s3_file_in_{folder_truncated}",
            bucket_name=BUCKET_NAME,
            bucket_key=s3_key,
            aws_conn_id=AWS_CONN_ID,
            timeout=24 * 60 * 60,
            poke_interval=30 * 60,
            mode="reschedule",
        )

        sensors.append(sensor)

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=DATALOADER_REPO,
        dag_id = "{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    
    #new_source_discounts
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

    build_docker_image = create_ssh_command_operator(
        task_id="build_image",
        command=f"docker build --no-cache --rm -t {IMAGE_NAME} {repository_name}/.",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_container_name = f"{{{{ dag.dag_id }}}}_container"
    run_docker_container = create_ssh_command_operator(
        task_id="run_docker_container",
        command=f"docker stop {docker_container_name};"
        f" docker rm {docker_container_name};"
        f" docker run --name {docker_container_name} -it -d -v '{DOCKER_VOLUME_PATH}:{CONTAINER_VOLUME_PATH}' {IMAGE_NAME}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    fetch_data = create_ssh_command_operator(
        task_id="fetch_data",
        command=f"docker exec -i {docker_container_name} python -m src.data_loader s3_pull --config_path dataloader.yaml",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker_container = create_ssh_command_operator(
        task_id="stop_docker_container",
        command=f"docker stop {docker_container_name} && docker rm {docker_container_name}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    for sensor in sensors:
        start >> sensor
        sensor >> clone_project
    (
        clone_project
        >> create_env_file
        >> build_docker_image
        >> run_docker_container
        >> fetch_data
        >> stop_docker_container
    )
