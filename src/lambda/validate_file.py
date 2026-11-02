import json
import boto3
import os
import urllib.request
import re

s3 = boto3.client("s3")

VALID_EXTENSIONS = [".csv"]
LANDING_PREFIX = "landing/"
RAW_PREFIX = "raw/"
REJECT_PREFIX = "rejected/"
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]


def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    print(f"Processing file : {key}")
    validate_and_move(bucket, key)


def validate_extension(key):
    for ext in VALID_EXTENSIONS:
        if key.endswith(ext):
            return True
    return False


def validate_filename(filename):
    pattern = r"^.*_\d{8}\.csv"
    return re.fullmatch(pattern, filename) is not None


def validate_size(bucket, key):
    obj = s3.head_object(
        Bucket=bucket,
        Key=key
    )
    size = obj["ContentLength"]
    if size == 0:
        return False
    return True


def move_file(bucket, source_key, destination_key):
    s3.copy_object(
        Bucket=bucket,
        CopySource={
            "Bucket": bucket,
            "Key": source_key
        },
        Key=destination_key
    )

    s3.delete_object(
        Bucket=bucket,
        Key=source_key
    )


def validate_and_move(bucket, key):
    filename = os.path.basename(key)
    if not validate_extension(key):
        reject(bucket, key, "Wrong file extension")
        return

    if not validate_filename(filename):
        reject(bucket, key, "Wrong filename")
        return

    if not validate_size(bucket, key):
        reject(bucket, key, "No data in file")
        return

    raw_key = key.replace(
        LANDING_PREFIX,
        RAW_PREFIX,
        1
    )

    move_file(
        bucket,
        key,
        raw_key
    )

    print("Moved to Raw")


def reject(bucket, key, reason):
    reject_key = key.replace(
        LANDING_PREFIX,
        REJECT_PREFIX,
        1
    )

    move_file(
        bucket,
        key,
        reject_key
    )

    send_slack_notification(
            bucket,
            reject_key,
            reason
        )
    print("Moved to Rejected")


def send_slack_notification(bucket, key, reason):
    message = {
        "text": (
            f"🚨 *File Validation Failed*\n\n"
            f"*Bucket:* {bucket}\n"
            f"*File:* {key}\n"
            f"*Reason:* {reason}"
        )
    }

    request = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(message).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        print(f"Slack response: {response.status}")