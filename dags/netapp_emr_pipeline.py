from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator


AWS_REGION = "us-east-1"
SUBNET_ID = "subnet-0e4578e2eeaecaca1"

SPARK_SCRIPT = (
    "s3://netapp-data-sample/"
    "spark_jobs/{{ dag_run.conf['spark_job'] }}"
)

#SPARK_SCRIPT = (
#    "s3://netapp-data-sample/spark_jobs/delta_test.py"
#)

INPUT_PATH = (
    "s3://{{ dag_run.conf['bucket'] }}/"
    "{{ dag_run.conf['key'] }}"
)

OUTPUT_PATH = (
    "s3://netapp-data-sample/"
    "processed/{{ dag_run.conf['file_type'] }}/"
)

JOB_FLOW_OVERRIDES = {
    "Name": "netapp-airflow-emr",
    "ReleaseLabel": "emr-7.0.0",
    "Applications": [
        {
            "Name": "Spark"
        }
    ],
    "BootstrapActions": [
    {
        "Name": "Install Python dependencies",
        "ScriptBootstrapAction": {
            "Path": "s3://netapp-data-sample/bootstrap/install_dependencies.sh"
        }
    }
   ],
    "Configurations": [
        {
            "Classification": "delta-defaults",
            "Properties": {
                "delta.enabled": "true"
            }
        }
    ],
    "Instances": {
        "Ec2SubnetId": SUBNET_ID,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "InstanceGroups": [
            {
                "Name": "SingleNode",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
           # {
           #     "Name": "Core",
           #     "Market": "ON_DEMAND",
           #     "InstanceRole": "CORE",
           #     "InstanceType": "m5.large",
           #     "InstanceCount": 1,
           # },
        ],
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://netapp-data-sample/emr-logs/",
    "Tags": [
    {
        "Key": "Project",
        "Value": "NetAppDataEngineering",
    },
    {
        "Key": "for-use-with-amazon-emr-managed-policies",
        "Value": "true",
    },
],
}

SPARK_STEP = [
    {
        "Name": "Run NetApp PySpark Job",
        "ActionOnFailure": "CONTINUE",  # TERMINATE_CLUSTER
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit", "--master", "yarn", "--deploy-mode", "client", 
                "--jars", "/usr/share/aws/delta/lib/delta-spark.jar",
                "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                SPARK_SCRIPT, "--input", INPUT_PATH, "--output", OUTPUT_PATH,
            ],
        },
    }
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="netapp_emr_pyspark_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # To avoid The number of vCPUs for instance type m5.xlarge exceeds the EC2 service quota for that type
    tags=[
        "netapp",
        "emr",
        "pyspark",
        "s3",
    ],
) as dag:

    create_emr = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    wait_for_emr = EmrJobFlowSensor(
        task_id="wait_for_emr",
        job_flow_id="{{ task_instance.xcom_pull("
        "task_ids='create_emr_cluster', "
        "key='return_value'"
        ") }}",
        target_states=[
            "WAITING"
        ],
        failed_states=[
            "TERMINATED_WITH_ERRORS",
        ],
        aws_conn_id="aws_default",
        # region_name=AWS_REGION,
    )

    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull("
        "task_ids='create_emr_cluster', "
        "key='return_value'"
        ") }}",
        steps=SPARK_STEP,
        aws_conn_id="aws_default",
        # region_name=AWS_REGION,
    )

    wait_for_spark = EmrStepSensor(
        task_id="wait_for_spark",
        job_flow_id="{{ task_instance.xcom_pull("
        "task_ids='create_emr_cluster', "
        "key='return_value'"
        ") }}",
        step_id="{{ task_instance.xcom_pull("
        "task_ids='add_spark_step', "
        "key='return_value'"
        ")[0] }}",
        target_states=[
            "COMPLETED"
        ],
        failed_states=[
            "FAILED",
            "CANCELLED",
            "INTERRUPTED",
        ],
        aws_conn_id="aws_default",
        # region_name=AWS_REGION,
    )

    terminate_emr = EmrTerminateJobFlowOperator(
        task_id="terminate_emr",
        job_flow_id="{{ task_instance.xcom_pull("
        "task_ids='create_emr_cluster', "
        "key='return_value'"
        ") }}",
        aws_conn_id="aws_default",
        # region_name=AWS_REGION,
        trigger_rule="all_done",
    )

    (
        create_emr
        >> wait_for_emr
        >> add_spark_step
        >> wait_for_spark
        >> terminate_emr
    )

