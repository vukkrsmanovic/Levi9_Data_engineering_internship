import json
import boto3
import time
import os

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

VALID_FOLDERS = ["pollution/", "sensor/", "weather/"]

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    for record in event["Records"]:
        try:
            body = json.loads(record["body"])
            s3_event = body["Records"][0]

            bucket = s3_event["s3"]["bucket"]["name"]
            key = s3_event["s3"]["object"]["key"]

            print(f"Processing file: {key} from bucket: {bucket}")

            if not key.startswith(tuple(VALID_FOLDERS)):
                raise Exception(f"Invalid folder: {key}")

            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read()

            print(f"Processed file {key}, size={len(content)} bytes")

            table_name = os.environ.get("DDB_TABLE")
            if not table_name:
                raise Exception("Missing DDB_TABLE environment variable")

            table = dynamodb.Table(table_name)

            timestamp = str(int(time.time()))

            table.put_item(
                Item={
                    "file_name": key,
                    "timestamp": timestamp,
                    "status": 0
                }
            )

            print(f"Inserted into DynamoDB: {key}, timestamp={timestamp}")

        except Exception as e:
            print(f"ERROR while processing message: {str(e)}")
            # Any exception causes Lambda to fail -> message -> DLQ (after retries)
            raise e

    return {"status": "ok"}
