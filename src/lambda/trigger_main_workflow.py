import json
import base64
import urllib.request
import urllib.error
import uuid
import boto3


AIRFLOW_URL = "http://172.31.39.87:8080"
SECRET_NAME = "netapp/airflow/api"

secrets = boto3.client(
    "secretsmanager",
    region_name="us-east-1"
)


def get_spark_job(key):

    filename = key.rsplit("/", 1)[-1].lower()

    if filename.startswith("volume_") and filename.endswith(".csv"):
        return "volume_transformations.py", "volume"

    if filename.startswith("cluster_") and filename.endswith(".csv"):
        return "cluster_transformations.py", "cluster"

    raise ValueError(f"Unknown file type: {filename}")


def lambda_handler(event, context):

    try:

        record = event["Records"][0]

        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f"Triggering Airflow for: s3://{bucket}/{key}")

        spark_job, file_type = get_spark_job(key)

        print(f"Spark job: {spark_job}")
        print(f"File type: {file_type}")

        # Get credentials from Secrets Manager
        print("Getting Airflow credentials from Secrets Manager")

        secret_response = secrets.get_secret_value(
            SecretId=SECRET_NAME
        )

        secret = json.loads(
            secret_response["SecretString"]
        )

        airflow_user = secret["username"]
        airflow_password = secret["password"]

        print("Airflow credentials obtained")

        # Airflow DAG payload
        payload = {
            "dag_run_id": f"s3_{uuid.uuid4().hex}",
            "conf": {
                "bucket": bucket,
                "key": key,
                "spark_job": spark_job,
                "file_type": file_type
            }
        }

        url = (
            f"{AIRFLOW_URL}/api/v1/dags/"
            f"netapp_emr_pyspark_pipeline/dagRuns"
        )

        print(f"Calling Airflow API: {url}")

        credentials = (
            f"{airflow_user}:{airflow_password}"
        )

        encoded_credentials = base64.b64encode(
            credentials.encode()
        ).decode()

        data = json.dumps(payload).encode("utf-8")

        request = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": (
                    f"Basic {encoded_credentials}"
                )
            }
        )

        print("Sending request to Airflow")

        with urllib.request.urlopen(
            request,
            timeout=10
        ) as response:

            response_body = response.read().decode("utf-8")

            print("Airflow response:")
            print(response_body)

            return {
                "statusCode": response.status,
                "body": response_body
            }

    except urllib.error.HTTPError as error:

        error_body = error.read().decode("utf-8")

        print("Airflow HTTP error:")
        print(error_body)

        return {
            "statusCode": error.code,
            "body": error_body
        }

    except urllib.error.URLError as error:

        print(f"Network error: {error}")

        return {
            "statusCode": 500,
            "body": str(error)
        }

    except Exception as error:

        print(f"Lambda error: {error}")

        return {
            "statusCode": 500,
            "body": str(error)
        }