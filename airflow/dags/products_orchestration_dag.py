from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import boto3
import requests
from botocore.config import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor


AWS_ENDPOINT = "http://floci:4566"
BUCKET = "ucv-data-platform"
GLUE_JOB = "products-bronze-to-silver"
ATHENA_DATABASE = "products_domain"


def aws_client(service: str):
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ingest_products(ds: str, ts_nodash: str, **_):
    response = requests.get(
        "https://dummyjson.com/products?limit=0",
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    key = (
        f"products/bronze/airflow/"
        f"ingestion_date={ds}/"
        f"products_{ts_nodash}.json"
    )

    s3 = aws_client("s3")
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Products received: {len(data['products'])}")
    print(f"Created: s3://{BUCKET}/{key}")
    return key


def start_glue_job(**_):
    glue = aws_client("glue")
    result = glue.start_job_run(JobName=GLUE_JOB)
    run_id = result["JobRunId"]
    print(f"Glue JobRunId: {run_id}")
    return run_id


def start_athena_query(ds: str, **_):
    athena = aws_client("athena")

    query = """
    SELECT
        category,
        product_count,
        average_price,
        average_rating
    FROM product_category_summary
    ORDER BY product_count DESC
    LIMIT 10
    """

    result = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={
            "OutputLocation": (
                f"s3://{BUCKET}/athena-results/airflow/{ds}/"
            )
        },
    )

    query_id = result["QueryExecutionId"]
    print(f"Athena QueryExecutionId: {query_id}")
    return query_id


def show_athena_results(**context):
    ti = context["ti"]
    query_id = ti.xcom_pull(task_ids="start_athena_query")

    athena = aws_client("athena")
    response = athena.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=20,
    )

    print("=== ATHENA RESULTS ===")
    for row in response["ResultSet"]["Rows"]:
        print([col.get("VarCharValue") for col in row["Data"]])


default_args = {
    "owner": "ucv-data-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="products_data_platform_orchestration",
    description="API -> S3 -> Glue state -> Athena validation",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ucv", "data-engineering", "floci"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_products_to_bronze",
        python_callable=ingest_products,
        op_kwargs={
            "ds": "{{ ds }}",
            "ts_nodash": "{{ ts_nodash }}",
        },
    )

    wait_bronze = S3KeySensor(
        task_id="wait_for_bronze_file",
        aws_conn_id="aws_floci",
        bucket_name=BUCKET,
        bucket_key=(
            "products/bronze/airflow/"
            "ingestion_date={{ ds }}/products_*.json"
        ),
        wildcard_match=True,
        poke_interval=5,
        timeout=60,
    )

    start_glue = PythonOperator(
        task_id="start_glue_job",
        python_callable=start_glue_job,
    )

    wait_glue = GlueJobSensor(
        task_id="wait_for_glue_job",
        aws_conn_id="aws_floci",
        job_name=GLUE_JOB,
        run_id="{{ ti.xcom_pull(task_ids='start_glue_job') }}",
        poke_interval=3,
        timeout=120,
    )

    start_athena = PythonOperator(
        task_id="start_athena_query",
        python_callable=start_athena_query,
        op_kwargs={"ds": "{{ ds }}"},
    )

    wait_athena = AthenaSensor(
        task_id="wait_for_athena_query",
        aws_conn_id="aws_floci",
        query_execution_id=(
            "{{ ti.xcom_pull(task_ids='start_athena_query') }}"
        ),
        sleep_time=3,
        timeout=120,
    )

    show_results = PythonOperator(
        task_id="show_athena_results",
        python_callable=show_athena_results,
    )

    ingest >> wait_bronze >> start_glue >> wait_glue
    wait_glue >> start_athena >> wait_athena >> show_results
