import re
import boto3
import requests
import pandas as pd
from botocore.exceptions import ClientError


# ---------------- CONFIG ----------------
SECRET_NAME = "tourist_estimate_token"
REGION_NAME = "eu-west-1"
API_URL = "https://rq5fbome43vbdgq7xoe7d6wbwa0ngkgr.lambda-url.eu-west-1.on.aws/"

BUCKET = "vuk-partitioned-bucket-2"
BASE_PREFIX = "pollution_partitioned/Ukraine"

FIXED_CITY = "Lviv" 
OUTPUT_BASE_PREFIX = "pollution_partitioned_enriched/Ukraine"

def get_tourist_api_token() -> str:
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=REGION_NAME)

    try:
        resp = client.get_secret_value(SecretId=SECRET_NAME)
    except ClientError as e:
        raise RuntimeError("Failed to fetch secret") from e

    return resp["SecretString"]


def fetch_estimate(for_date: str) -> int:
    token = get_tourist_api_token()

    headers = {"Authorization": f"Bearer {token}"}
    params = {"date": for_date}

    r = requests.get(API_URL, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    for item in data.get("info", []):
        if item["name"] == FIXED_CITY:
            return item["estimated_no_people"]

    return None


def list_date_folders():
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=BASE_PREFIX + "/", Delimiter="/")

    dates = []

    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            prefix = cp["Prefix"]
            m = re.search(r"date=(\d{4}-\d{2}-\d{2})", prefix)
            if m:
                dates.append(m.group(1))

    return sorted(dates)


def find_parquet_for_date(date_str: str) -> str:
    s3 = boto3.client("s3")
    prefix = f"{BASE_PREFIX}/date={date_str}/"

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    if "Contents" not in resp:
        raise RuntimeError(f"No objects found in {prefix}")

    for obj in resp["Contents"]:
        key = obj["Key"]
        if key.endswith(".parquet"):
            return key

    raise RuntimeError(f"No parquet file found in {prefix}")


def main():
    print("Listing date folders...")
    dates = list_date_folders()
    print("Found dates:", dates)

    for d in dates:
        print(f"\n=== Processing date {d} ===")

        # 1) API poziv
        est = fetch_estimate(d)
        print(f"Tourist estimate for {FIXED_CITY} on {d}: {est}")

        # 2) Pronadji parquet za taj datum
        parquet_key = find_parquet_for_date(d)
        input_uri = f"s3://{BUCKET}/{parquet_key}"
        print(f"Reading parquet from: {input_uri}")

        # 3) Učitaj kao pandas DataFrame
        df = pd.read_parquet(input_uri, storage_options={"anon": False})

        # 4) Dodaj kolonu
        df["tourist_estimate"] = est

        # 5) Upis nazad
        out_prefix = f"{OUTPUT_BASE_PREFIX}/date={d}"
        out_key = f"{out_prefix}/data.parquet"
        output_uri = f"s3://{BUCKET}/{out_key}"

        print(f"Writing enriched parquet to: {output_uri}")
        df.to_parquet(
            output_uri,
            index=False,
            storage_options={"anon": False}
        )


if __name__ == "__main__":
    main()
