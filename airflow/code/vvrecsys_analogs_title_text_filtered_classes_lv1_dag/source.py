from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.ssh.operators.ssh import SSHOperator

from operators.remote_operator import (
    create_clone_repository_operator,
    create_docker_start_operator,
    create_docker_stop_operator,
    create_python_in_docker_ssh_command,
    create_ssh_command_operator,
)

GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

FEATURES_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys-ad-hoc.git"
REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_prod_connection_backup"

INSTANCE_ID = Variable.get("PROD_INSTANCE_ID")
AUTHORIZED_KEYS = Variable.get("AUTHORIZED_KEYS", deserialize_json=True)

CLOUD_DATA_PATH = "/srv/data"
OPENVPN_FOLDER_PATH = "/etc/openvpn"
SSH_FOLDER_PATH = "/home/airflow/.ssh"

TELEGRAM_TOKEN_BOT = Variable.get("TELEGRAM_TOKEN_BOT")
TELEGRAM_SENDING_TOKEN = Variable.get("TELEGRAM_SENDING_TOKEN")

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
    "vvrecsys_analogs_title_text_filtered_classes_lv1_dag",
    default_args={
        "depends_on_past": False,
        # "on_failure_callback": create_telegram_alert_func(TELEGRAM_TOKEN_BOT),
        "owner": "v.zelenin",
    },
    description="DAG for inference analogs_title_text filtered by class_lv1",
    schedule_interval="15 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["recsys", "analogs_text", "analogs_title", "filtered", "prod"],
) as dag:
    start = DummyOperator(task_id="start")

    year = "{{ macros.ds_add(ds, 2).split('-')[0] }}"
    month = "{{ macros.ds_add(ds, 2).split('-')[1].zfill(2) }}"
    day = "{{ macros.ds_add(ds, 2).split('-')[2].zfill(2) }}"

    s3_sensor = S3KeySensor(
        task_id="check_s3_files",
        bucket_name=BUCKET_NAME,
        bucket_key=f"recsys3/analogs_text/year={year}/month={month}/day={day}/analogs_100_filtered_title.json",
        aws_conn_id=AWS_CONN_ID,
        timeout=24 * 60 * 60,
        poke_interval=30 * 60,
        mode="reschedule",
        wildcard_match=False,
    )

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=FEATURES_PIPELINE_REPO,
        dag_id="{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name="python311",
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

    create_tg_file = create_ssh_command_operator(
        task_id="create_tg_file",
        command=(f'echo "token: {TELEGRAM_SENDING_TOKEN}" > {repository_name}/.credentials/tg.yaml'),
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
        cpus=16,
    )

    build_analogs_text = create_python_in_docker_ssh_command(
        task_id="build_analogs_text",
        command="python -m src.cli.run --script=analogs_text --start_date={{ macros.ds_add(ds, 2) }} --end_date={{ macros.ds_add(ds, 2) }} --config=./configs/analogs_title_text_filtered_classes_lv1.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=3 * 60 * 60,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    filter_analogs_text = create_python_in_docker_ssh_command(
        task_id="filter_analogs_text",
        command="python -m src.cli.run --script=analogs_text_filter --start_date={{ macros.ds_add(ds, 2) }} --end_date={{ macros.ds_add(ds, 2) }} --config=./configs/analogs_title_text_filtered_classes_lv1.yaml",
        docker_container_name=docker_container_name,
        cmd_timeout=30 * 60,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    upload_analogs_title_filtered_class_lv1 = create_ssh_command_operator(
        task_id="upload_analogs_title_filtered_class_lv1",
        command=(
            f"docker exec -i {docker_container_name} python -m vvrecsys.cli.minio --config='./configs/analogs_title_text_filtered_classes_lv1.yaml' --focus_group=3 --coitems"
        ),
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )

    conn = BaseHook.get_connection(AWS_CONN_ID)
    aws_access_key = conn.login
    aws_secret_key = conn.password
    recommends_folder = "/srv/data/production/analogs_text"

    copy_files_to_s3 = SSHOperator(
        task_id="copy_h5_to_s3",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        command="""
        export AWS_ACCESS_KEY_ID={{ params.aws_access_key }}
        export AWS_SECRET_ACCESS_KEY={{ params.aws_secret_key }}
        aws s3 cp /srv/data/production/analogs_text/ s3://{{ params.bucket_name }}/recsys3/production/{{ ds.replace('-', '/') }}/analogs_text/ --recursive --exclude '*' --include '*.parquet' --endpoint-url=https://storage.yandexcloud.net
        """,
        params={
            "aws_access_key": AWS_ACCESS_KEY_ID,
            "aws_secret_key": AWS_SECRET_ACCESS_KEY,
            "bucket_name": BUCKET_NAME,
        },
    )

    (
        start
        >> s3_sensor
        >> clone_project
        >> create_env_file
        >> create_minio_file
        >> create_tg_file
        >> start_docker
        >> build_analogs_text
        >> filter_analogs_text
        >> upload_analogs_title_filtered_class_lv1
        >> stop_docker
        >> copy_files_to_s3
    )
