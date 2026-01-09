import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import create_ssh_command_operator
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys-ad-hoc.git"
IMAGE_NAME = "lifecycle_cleanup_dag"

REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"

DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")


with DAG(
    "vvrecsys_lifecycle_cleanup_dag",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "v.zelenin",
    },
    description="Daily cleanup of Docker cache (image, builder) and git repositories",
    schedule_interval="00 18 * * *",
    start_date=datetime(2024, 7, 9),
    catchup=False,
    tags=["cleanup", "docker", "git"],
) as dag:
    start = DummyOperator(
        task_id="start",
    )

    check_docker_cleanup = create_ssh_command_operator(
        task_id="check_docker_cleanup",
        command="docker system df",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    cleanup_docker = create_ssh_command_operator(
        task_id="cleanup_docker",
        command="docker image prune -f && docker builder prune -f",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    cleanup_git_repositories = create_ssh_command_operator(
        task_id="cleanup_git_repositories",
        command="find /home/airflow -mindepth 1 -maxdepth 1 -type d -exec test -d {}/.git \; -print -exec rm -rf {} +",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    repository_name = "{{ dag.dag_id }}_recsys-ad-hoc"
    clone_project = create_ssh_command_operator(
        task_id="clone_project",
        command=f"rm -rf {repository_name}; git clone -b python311 {FEATURES_PIPELINE_REPO} {repository_name};",
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
        command=f"docker build --rm -t {IMAGE_NAME} {repository_name}/.",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_container_name = "{{ dag.dag_id }}_container"
    run_docker_container = create_ssh_command_operator(
        task_id="run_docker_container",
        command=f"docker stop {docker_container_name};"
        f" docker rm {docker_container_name};"
        f" docker run --name {docker_container_name} -it -d -v '{DOCKER_VOLUME_PATH}:/data' {IMAGE_NAME}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    h5_datasets_paths = [
        "/data/processed/datasets.h5",
        "/data/processed/datasets_coitems.h5",
        "/data/processed/datasets_analogues.h5",
    ]

    cleanup_tasks = []
    for dataset_path in h5_datasets_paths:
        dataset_name = os.path.splitext(os.path.basename(dataset_path))[0]
        cleanup_task = create_ssh_command_operator(
            task_id=f"cleanup_{dataset_name}",
            command=f"docker exec -i {docker_container_name} "
            f"python /app/src/scripts/h5_cleanup.py "
            f"{dataset_path} "
            f"--date {{{{ ds }}}} "
            f"--config=configs/h5_cleanup.yaml",
            ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        )
        cleanup_tasks.append(cleanup_task)

    stop_docker_container = create_ssh_command_operator(
        task_id="stop_docker_container",
        command=f"docker stop {docker_container_name} && docker rm {docker_container_name}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    repack_tasks = []
    for dataset_path in h5_datasets_paths:
        dataset_name = os.path.splitext(os.path.basename(dataset_path))[0]
        temp_path = f"/srv/data/processed/{dataset_name}_temp.h5"
        repack_task = create_ssh_command_operator(
            task_id=f"repack_{dataset_name}",
            command=f"sudo h5repack -v /srv{dataset_path} {temp_path} && sudo mv {temp_path} /srv{dataset_path}",
            ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        )
        repack_tasks.append(repack_task)

    (
        start
        >> check_docker_cleanup
        >> cleanup_docker
        >> cleanup_git_repositories
        >> clone_project
        >> create_env_file
        >> build_docker_image
        >> run_docker_container
        >> cleanup_tasks
        >> stop_docker_container
        >> repack_tasks
    )
