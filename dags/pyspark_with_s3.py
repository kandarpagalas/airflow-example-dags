from __future__ import annotations

import pendulum, os, tempfile
from datetime import timedelta
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

# Amazon
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

EXEC_DATE = '{{ ds_nodash }}'

default_args={
    "email_on_failure": False,

    "email_on_retry": False,
    "email": Variable.get("infrastructure_manager_email"),
    # "retries": 3,
    # 'retry_delay': datetime.timedelta(minutes=1)
    }

with DAG(
    dag_id="pyspark_with_s3",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Fortaleza"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    tags=["example", "spark"],
) as dag:

    @task
    def load_data():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
                            .master("local") \
                            .appName("Spark_S3") \
                            .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
                            .config("fs.s3a.endpoint", Variable.get('AWS_ENDPOINT_URL')) \
                            .config("fs.s3a.access.key", Variable.get('AWS_ACCESS_KEY_ID')) \
                            .config("fs.s3a.secret.key", Variable.get('AWS_SECRET_ACCESS_KEY'))\
                            .getOrCreate()
        
        ## Read from ingest folder
        s3path = 's3a://bucket_name/*.json'
        march_df = spark.read.json(s3path, multiLine=True)
        return(march_df)
    my_data = load_data()
    
    @task
    def preprocessing(spark_data):
        print(type(spark_data))
        spark_data.printSchema()
        
    my_data2 = preprocessing(my_data)
        

if __name__ == "__main__":
    dag.test()
    
