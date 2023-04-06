from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator

EXEC_DATE = '{{ ds_nodash }}'

with DAG(
    dag_id="S3KeySensor",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Fortaleza"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    params={"example_key": "example_value"},
) as dag:
    sensor_minio_s3 = S3KeySensor(
        task_id="sensor_minio_s3",
        bucket_name="airflow", # Name of the S3 bucket
        bucket_key="data.csv", # The key being waited on. Supports full s3:// style url or relative path from root leve
        aws_conn_id="minio_s3_conn", # a reference to the s3 connection
        #wildcard_match="run_this_first", # whether the bucket_key should be interpreted as a Unix wildcard pattern
        #daverify="run_this_first" # Whether or not to verify SSL certificates for S3 connection. By default SSL certificates are verified. You can provide the following values
    )
    
    obj_copy = S3CopyObjectOperator(
        task_id="obj_copy",
        aws_conn_id="minio_s3_conn",
        # Source
        source_bucket_name="airflow", 
        source_bucket_key="data.csv", 
        # Dest
        dest_bucket_key=f"copy/{EXEC_DATE}_cpy.csv", 
        dest_bucket_name="airflow"
    )
    
    # del_obj = S3DeleteObjectsOperator(
    #     task_id="del_obj",
    #     aws_conn_id="minio_s3_conn",
    #     bucket = "airflow", # (str) – Name of the bucket in which you are going to delete object(s). (templated)
    #     keys = f"copy/{EXEC_DATE}.csv", # (str | list | None) key o list[str] of objects to delete.
    #     #prefix="" # (str | None) – Prefix of objects to delete. (templated) All objects matching this prefix in the bucket will be deleted.
    #     # Dest
    # )
    
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    sensor_minio_s3 >> run_this_last
    sensor_minio_s3 >> obj_copy #>> del_obj

if __name__ == "__main__":
    dag.test()