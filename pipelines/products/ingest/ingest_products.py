import json
from datetime import datetime, timezone

import boto3
import requests
from botocore.config import Config

API_URL = "https://dummyjson.com/products?limit=0"

S3_ENDPOINT = "http://localhost:4566"
BUCKET = "ucv-data-platform"
DOMAIN = "products"


def get_products():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if "products" not in payload:
        raise ValueError("The API response does not contain 'products'.")

    print(f"HTTP status: {response.status_code}")
    print(f"Products received: {len(payload['products'])}")

    return payload


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def upload_bronze(payload):
    now = datetime.now(timezone.utc)

    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_ts = now.strftime("%Y%m%dT%H%M%SZ")

    key = (
        f"{DOMAIN}/bronze/"
        f"ingestion_date={ingestion_date}/"
        f"products_{ingestion_ts}.json"
    )

    body = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2
    ).encode("utf-8")

    s3 = get_s3_client()

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    print(f"Bronze object created: s3://{BUCKET}/{key}")
    return key


if __name__ == "__main__":
    data = get_products()
    upload_bronze(data)
