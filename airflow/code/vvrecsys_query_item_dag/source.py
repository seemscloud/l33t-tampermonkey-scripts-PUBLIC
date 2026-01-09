from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_stop_operator,
    create_ssh_command_operator,
)
from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/vvnprecsys.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
IMAGE_NAME = "query_item:vpn"
INSTANCE_ID = Variable.get("PROD_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

DOCKER_VOLUME_PATH = Variable.get("PROD_DOCKER_VOLUME_PATH")
TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")

ENDPOINT_URL = Variable.get("ENDPOINT_URL")
# REGION_NAME = Variable.get("REGION_NAME")
REGION_NAME = "ru-vv"

AWS_CONN_ID = "s3_experimental"
BUCKET_NAME = Variable.get("BUCKET_NAME")

OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
OPENVPN_CREDENTIALS_PATH = "/etc/openvpn"


# def generate_wait_vpn_command(container_name, max_wait_seconds=60 * 20):
#     return f"""
#     set -e
#     echo "Ожидание инициализации VPN..."
#     SECONDS=0
#     until docker logs {container_name} 2>&1 | grep "Initialization Sequence Completed"; do
#         if [ $SECONDS -ge {max_wait_seconds} ]; then
#             echo "VPN не поднялся за {max_wait_seconds} секунд"
#             exit 1
#         fi
#         sleep 2
#     done
#     echo "VPN поднялся!"
#     """


with DAG(
    "vvrecsys_query_item_dag",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "a.velichko",
    },
    description="DAG for query item ranking",
    schedule_interval="30 02 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "query item_ranking", "prod"],
    params={
        "logic_date": Param(default="", type="string", description="Дата запуска (для сенсоров)"),
    },
) as dag:
    start = DummyOperator(task_id="start")

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
            f"MINIO_ACCESS_KEY={MINIO_ACCESS_KEY}\n"
            f"MINIO_SECRET_KEY={MINIO_SECRET_KEY}\n"
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}' > {repository_name}/.env"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_docker_image = create_ssh_command_operator(
        task_id="build_image",
        command=f"docker build --no-cache -f {repository_name}/docker/Dockerfile.vpn --rm -t {IMAGE_NAME} {repository_name}/.",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    VPN_FILEPATH = "/etc/openvpn/vks-vpn02.ovpn"
    SLEEP_TIME = 15
    start_vpn_connection = create_ssh_command_operator(
        task_id="start_vpn_connection",
        command=f"pgrep openvpn && echo 'VPN уже работает' || (echo 'Запуск VPN' && sudo openvpn --config {VPN_FILEPATH} --daemon) && sleep {SLEEP_TIME}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    docker_container_name = "{{ dag.dag_id }}_container"

    run_docker_container = create_ssh_command_operator(
        task_id="run_docker_container",
        command=(
            f"docker stop {docker_container_name} || true && "
            f"docker rm {docker_container_name} || true && "
            f"docker run --cap-add=NET_ADMIN --device /dev/net/tun --name {docker_container_name} -it -d "
            f"-v '{DOCKER_VOLUME_PATH}:/data' "
            f"-v '/etc/openvpn:/creds' "
            f"{IMAGE_NAME}"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    run_query_item_ranking = create_ssh_command_operator(
        task_id="run_query_item_ranking",
        command=(
            f"docker exec -i {docker_container_name} python -m src.cli.query_item --config='./configs/query_item.yaml' --date={{{{ (params.logic_date if params.logic_date else ds) | replace('-', '/') }}}}"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    start >> clone_project

(
    clone_project
    >> create_env_file
    >> build_docker_image
    >> start_vpn_connection
    >> run_docker_container
    >> run_query_item_ranking
    >> stop_docker
)
