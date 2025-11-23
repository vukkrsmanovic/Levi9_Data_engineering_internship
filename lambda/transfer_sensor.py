import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    dest_bucket = os.environ['DEST_BUCKET']
    source_folder = os.environ['SOURCE_FOLDER']
    dest_folder = os.environ['DEST_FOLDER']

    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_folder):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            src_key = obj["Key"]

            # skip "folders"
            if src_key.endswith("/"):
                continue

            dst_key = src_key.replace(source_folder, dest_folder, 1)

            print(f"Copying {src_key} → {dst_key}")

            s3.copy_object(
                Bucket=dest_bucket,
                CopySource={'Bucket': source_bucket, 'Key': src_key},
                Key=dst_key
            )

    return {"message": "Transfer complete"}
