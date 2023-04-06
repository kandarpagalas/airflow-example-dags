from __future__ import annotations

import pendulum
from datetime import timedelta
from docker.types import Mount

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args={
    "owner": "examples"
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "email": "example@email.com",
    # "retries": 3,
    # 'retry_delay': timedelta(minutes=1)
    }

with DAG(
    dag_id="DockerOperator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args=default_args,
    params={"example_key": "example_value"},
    tags=["example"],
) as dag:
    message_task = DockerOperator(
        task_id="docker_message_task",
        image='example-docker-etl:latest',
        api_version='auto',
        auto_remove=True,
        command="python src/task.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        # Mount volume, this way you dont need to rebuild after code changes
        mounts=[
            Mount(source='/root/airflow/dags/sidtips/docker_containers/oddsportal/src/',
                  target='/app/src',
                  type='bind')
        ],
        # Solution to read XComs output after task execution
        retrieve_output=True,
        retrieve_output_path="/app/output.out",
        # Add environment variables to container
        environment={
                  'DAY': '{{ ds_nodash }}'
                },
    )

if __name__ == "__main__":
    dag.test()