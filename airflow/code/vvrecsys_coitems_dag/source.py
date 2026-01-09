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

from utils.telegram import create_telegram_alert_func

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys-ad-hoc.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection"

INSTANCE_ID = Variable.get("PROD_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"

TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

ENDPOINT_URL = Variable.get("ENDPOINT_URL")
REGION_NAME = Variable.get("REGION_NAME")

AWS_CONN_ID = "s3_experimental"
BUCKET_NAME = Variable.get("BUCKET_NAME")

MINIO_ENDPOINT_URL = Variable.get("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")

with DAG(
    "vvrecsys_coitems_dag",
    default_args={
        "depends_on_past": False,
        "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "s.nikiforov",
    },
    description="DAG for inference coitems recs",
    schedule_interval="30 7 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "coitems", "prod"],
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
        repository_url=FEATURES_PIPELINE_REPO,
        dag_id = "{{ dag.dag_id }}",
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
    
    docker_container_name, start_docker = create_docker_start_operator(
        task_id="start_docker",
        dag_id="{{ dag.dag_id }}",
        repository_name=repository_name,
        volume=[f"{CLOUD_DATA_PATH}:/app/data", f"{OPENVPN_FOLDER_PATH}:/app/etc/openvpn", f"{SSH_FOLDER_PATH}:/root/.ssh"],
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        cpus=16,
    )
    
    build_tfidf = create_python_in_docker_ssh_command(
        task_id="build_tfidf",
        command="python -m src.cli.run --script=coitems --start_date={{ macros.ds_add(ds, -270) }} --end_date={{ macros.ds_add(ds, -1) }} --config=./configs/coitems.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    
    # build_bm25 = create_python_in_docker_ssh_command(
    #     task_id="build_bm25",
    #     command="python -m src.cli.run --script=coitems_bm25 --start_date={{ macros.ds_add(ds, -270) }} --end_date={{ macros.ds_add(ds, -1) }} --config=./configs/coitems_bm25.yaml",
    #     docker_container_name=docker_container_name,
    #     cmd_timeout=1 * 60 * 60 * 4,  # 4 hours
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )
    
    upload_tfidf = create_ssh_command_operator(
        task_id="upload_tfidf",
        command=(
             f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config='./configs/coitems.yaml' --coitems"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    
    # upload_bm25 = create_ssh_command_operator(
    #     task_id="upload_bm25",
    #     command=(
    #          f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config='./configs/coitems_bm25.yaml' --coitems --focus_group=2"
    #     ),
    #     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    # )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    conn = BaseHook.get_connection(AWS_CONN_ID)
    aws_access_key = conn.login
    aws_secret_key = conn.password
    recommends_folder = (
        "/srv/data/production/coitems"
    )
    s3_folder = "recsys3/models/coitems"

    copy_files_to_s3 = SSHOperator(
        task_id="copy_h5_to_s3",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        command="""
        export AWS_ACCESS_KEY_ID={{ params.aws_access_key }}
        export AWS_SECRET_ACCESS_KEY={{ params.aws_secret_key }}
        aws s3 cp /srv/data/production/coitems/ s3://{{ params.bucket_name }}/recsys3/production/{{ ds.replace('-', '/') }}/coitems/ --recursive --exclude '*' --include '*.parquet' --endpoint-url=https://storage.yandexcloud.net
        """,
        params={
            "aws_access_key": AWS_ACCESS_KEY_ID,
            "aws_secret_key": AWS_SECRET_ACCESS_KEY,
            "bucket_name": BUCKET_NAME,
        },
    )

    (
        start
        >> check_file_sensor
        >> clone_project
        >> create_env_file
        >> create_minio_file
        >> start_docker
        >> build_tfidf
        # >> build_bm25
        >> upload_tfidf
        # >> upload_bm25
        >> stop_docker
        >> copy_files_to_s3
    )
