from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args={
    "owner": "examples"
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "email": "example@email.com",
    # "retries": 3,
    # 'retry_delay': timedelta(minutes=1)
    }


with DAG(
    dag_id="EmptyOperator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Fortaleza"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args=default_args,
    params={"example_key": "example_value"},
    tags=["example"],
) as dag:
    run_this_first = EmptyOperator(
        task_id="run_this_first",
    )
    
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    run_this_first >> run_this_last

if __name__ == "__main__":
    dag.test()