from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base_hook import BaseHook

from operators.compute_cloud_operator import start_compute_cloud_operator
from operators.remote_operator import create_ssh_command_operator, RemoteFileSensor, create_clone_repository_operator, create_docker_stop_operator
from utils.telegram import create_telegram_alert_func


GITLAB_TOKEN_VV = Variable.get("GITLAB_TOKEN_VV")
GITLAB_URL_VV = Variable.get("GITLAB_URL_VV")

login_mlflow = Variable.get("login_mlflow")
password_mlflow = Variable.get("password_mlflow")

REMOTE_HOST_MACHINE_SSH_CONNECTION = "ssh_recsys_dev_connection"
INSTANCE_ID = Variable.get('DEV_INSTANCE_ID')
AUTHORIZED_KEYS = Variable.get('AUTHORIZED_KEYS', deserialize_json=True)
QUALITY_PIPELINE_REPO = f"https://{GITLAB_TOKEN_VV}:{GITLAB_TOKEN_VV}@{GITLAB_URL_VV}/recsys_data_quality_check.git"
IMAGE_NAME = "data_quality_check:latest"
DOCKER_VOLUME_PATH = Variable.get("DEV_DOCKER_VOLUME_PATH")

# AWS_CONN_ID = "mlflow"

# conn = BaseHook.get_connection(AWS_CONN_ID)
# aws_access_key = conn.login
# aws_secret_key = conn.password
    
with DAG(
    'dev_data_quality_check_dag',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        "on_failure_callback": create_telegram_alert_func("6573050267:AAHXSde3V8eFJL_JZufBS51QXF2GZ2_A9wA"),
        'retry_delay': timedelta(minutes=5),
        'owner': 'm.gazoian'
    },
    description='DAG for data quality check',
    schedule_interval='30 18 * * *',
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=["recsys", 'data', 'check' "dev"],
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    data_folder = "/srv/data/raw"
    parquet_path = f"{data_folder}/sales/{{{{ ds.replace('-', '/') }}}}.parquet"
    
    check_file_sensor = RemoteFileSensor(
        task_id="check_remote_file_in_sales",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        remote_filepath=parquet_path,
        timeout=24 * 60 * 60,    
        poke_interval=30 * 60,    
        mode="reschedule",        
    )
    start_compute_cloud = start_compute_cloud_operator(
        task_id='start_compute_cloud',
        instance_id=INSTANCE_ID,
        authorized_keys=AUTHORIZED_KEYS,
    )

    repository_name, clone_project = create_clone_repository_operator(
        task_id="clone_git",
        repository_url=QUALITY_PIPELINE_REPO,
        dag_id = "{{ dag.dag_id }}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
        branch_name='dev'
    )

    build_docker_image = create_ssh_command_operator(task_id="build_image",
                                                     command=f"docker build --no-cache --rm -t {IMAGE_NAME} recsys_data_quality_check/.",
                                                     ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION)

    docker_container_name = "recsys_data_quality_check_run_container"
    run_docker_container = create_ssh_command_operator(
        task_id="run_docker_container",
        command=f"docker stop {docker_container_name};"
                f" docker rm {docker_container_name};"
                f" docker run --name {docker_container_name} -it -d -v '{DOCKER_VOLUME_PATH}:/srv/data' {IMAGE_NAME}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION
    )
    
    run_data_quality_check = create_ssh_command_operator(
        task_id="run_data_quality_check",
        command=f"docker exec -i {docker_container_name} python -m src.data_quality_check {{{{ ds }}}} {login_mlflow} {password_mlflow}",
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION
    )
    
    stop_docker = create_docker_stop_operator(
        task_id="stop_docker",
        container_name=docker_container_name,
        ssh_conn_id=REMOTE_HOST_MACHINE_SSH_CONNECTION,
    )
    
    start >> check_file_sensor >> start_compute_cloud >> clone_project >>  build_docker_image >> run_docker_container >> run_data_quality_check >> stop_docker
