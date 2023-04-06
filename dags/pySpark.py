from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

# Amazon
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

EXEC_DATE = '{{ ds_nodash }}'

with DAG(
    dag_id="Spark_S3Hook",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Fortaleza"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=["example", "spark"],
    params={"example_key": "example_value"},
) as dag:
    @task
    def pyspark_test():
        import os, tempfile
        import pandas as pd
        from io import StringIO
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
                            .master("local") \
                            .appName("Spark_S3Hook") \
                            .getOrCreate()
        
        s3_hook = S3Hook(aws_conn_id = 'minio_s3_conn')

        file_content = s3_hook.read_key(
            key = 'folder1/football_matches.csv',
            bucket_name = 'bucket_name'
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csvStringIO = StringIO(file_content)
            df = pd.read_csv(csvStringIO)
            df.to_csv('test.csv')
            
            spark_df = spark.read.csv('test.csv', header=True)
            spark_df.printSchema()

    my_spark = pyspark_test()

if __name__ == "__main__":
    dag.test()