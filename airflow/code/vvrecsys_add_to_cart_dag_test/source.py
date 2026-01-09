from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from operators.remote_operator import (
    RemoteFileSensor,
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_python_in_docker_ssh_command,
    create_ssh_command_operator,
)

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")
GITLAB_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/vvrecsys.git"
INSTANCE_ID = Variable.get("INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"
CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"
OPENVPN_CREDENTIALS_PATH = "/etc/openvpn"
#TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
#TELEGRAM_SENDING_TOKEN = Variable.get("TELEGRAM_SENDING_TOKEN")
MINIO_ENDPOINT_URL = Variable.get("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
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

AWS_CONN_ID = "s3_experimental"

with DAG(
        "vvrecsys_add_to_cart_dag_test",
        default_args={
            "depends_on_past": False,
        #    "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
            "owner": "a.velichko",
        },
        description="DAG for inference vvrecsys_add_to_cart_dag_test",
        schedule_interval="30 08 * * *",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["recsys", "add_to_cart", "prod"],
) as dag:
    start = DummyOperator(task_id="start")

    data_folder = "/srv/data/raw"
    sales_folder = "sales"
    parquet_path = "{{ yesterday_ds.replace('-', '/') }}.parquet"

    check_file_sensor = RemoteFileSensor(
        task_id=f"check_remote_file_in_{sales_folder}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        remote_filepath=f"{data_folder}/{sales_folder}/{parquet_path}",
        timeout=24 * 60 * 60,
        poke_interval=30 * 60,
        mode="reschedule",
    )

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=GITLAB_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name='fix-add-to-cart'
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

    create_minio_file = create_ssh_command_operator(
        task_id="create_minio_file",
        command=(
            f"mkdir -p {repository_name}/.credentials;\n"
            f"echo 'endpoint: {MINIO_ENDPOINT_URL}\n"
            f"access_key: {MINIO_ACCESS_KEY}\n"
            f"secret_key: {MINIO_SECRET_KEY}' > {repository_name}/.credentials/minio.yaml"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    #
    # create_tg_file = create_ssh_command_operator(
    #     task_id="create_tg_file",
    #     command=(
    #         f'echo "token: {TELEGRAM_SENDING_TOKEN}" '
    #         f'> {repository_name}/.credentials/tg.yaml'
    #     ),
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )

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
        cpus=16,
    )

    set_date = create_python_in_docker_ssh_command(
        task_id="set_date",
        command="python -m vvrecsys.cli.set_date  --config=configs/datasets.yaml --date {{ macros.ds_add(ds, -1) }}",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 1,  # 1 hour
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    build_add_to_cart_window_feats = create_python_in_docker_ssh_command(
        task_id="build_add_to_cart_window_feats",
        command="python -m vvrecsys.cli.production --config=configs/production/add_to_cart_ranking_with_window_feats.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    # build_merge_candidates = create_python_in_docker_ssh_command(
    #     task_id="build_add_to_cart_window_feats",
    #     command="python -m vvrecsys.cli.merge_candidates --config=configs/merge/iptop_tfidf_gtop_fallback.yaml",
    #     docker_container_name=docker_container_name,
    #     cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )

    # build_add_to_cart_improved_model_and_fallback = create_python_in_docker_ssh_command(
    #     task_id="build_add_to_cart_improved_model_and_fallback",
    #     command="python -m vvrecsys.cli.production --config=configs/production/add_to_cart_improved_model_and_fallback.yaml",
    #     docker_container_name=docker_container_name,
    #     cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )

    # deploy_add_to_cart_window_feats = create_ssh_command_operator(
    #     task_id="deploy_add_to_cart_window_feats",
    #     command=(
    #         f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config=configs/production/add_to_cart_ranking_with_window_feats.yaml --index=0 --focus_group=3"
    #     ),
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )

    # deploy_add_to_cart_improved_model_and_fallback = create_ssh_command_operator(
    #     task_id="deploy_add_to_cart_improved_model_and_fallback",
    #     command=(
    #         f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config=configs/production/add_to_cart_improved_model_and_fallback.yaml --index=0 --focus_group=2"
    #     ),
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    # conn = BaseHook.get_connection(AWS_CONN_ID)
    # aws_access_key = conn.login
    # aws_secret_key = conn.password
    # recommends_folder = "/srv/data/production/add_to_cart"
    # s3_folder = "recsys3/models/add_to_cart"
    #
    # copy_files_to_s3 = SSHOperator(
    #     task_id="copy_h5_to_s3",
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    #     command="""
    #     export AWS_ACCESS_KEY_ID={{ params.aws_access_key }}
    #     export AWS_SECRET_ACCESS_KEY={{ params.aws_secret_key }}
    #     aws s3 cp /srv/data/production/add_to_cart/ s3://{{ params.bucket_name }}/recsys3/production/{{ macros.ds_add(ds, -1).replace('-', '/') }}/add_to_cart/ --recursive --exclude '*' --include '*.parquet' --endpoint-url=https://storage.yandexcloud.net
    #     """,
    #     params={
    #         "aws_access_key": AWS_ACCESS_KEY_ID,
    #         "aws_secret_key": AWS_SECRET_ACCESS_KEY,
    #         "bucket_name": BUCKET_NAME,
    #     },
    # )

    (
            start
            >> check_file_sensor
            >> clone_project
            >> create_env_file
            >> create_minio_file
          #  >> create_tg_file
            >> start_docker
            >> set_date
            >> build_add_to_cart_window_feats
          #  >> deploy_add_to_cart_window_feats
            # >> build_add_to_cart_improved_model_and_fallback
            # >> deploy_add_to_cart_improved_model_and_fallback
            >> stop_docker
          #  >> copy_files_to_s3
    )
