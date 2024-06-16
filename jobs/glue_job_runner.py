from jobs.aws_secret_manager import get_secret
from jobs.glue_job_submission import create_glue_job
import os

s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
tabular_credential = get_secret("TABULAR_CREDENTIAL")
catalog_name = get_secret("CATALOG_NAME")  # "eczachly-academy-warehouse"
aws_region = get_secret("AWS_GLUE_REGION")  # "us-west-2"
aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = get_secret("KAFKA_CREDENTIALS")

def create_and_run_glue_job(job_name, script_path, arguments):
    glue_job = create_glue_job(
                    job_name=job_name,
                    script_path=script_path,
                    arguments=arguments,
                    aws_region=aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    tabular_credential=tabular_credential,
                    s3_bucket=s3_bucket,
                    catalog_name=catalog_name,
                    kafka_credentials=kafka_credentials
                    )



local_script_path = os.path.join("jobs", 'batch/kafka_spark_streaming_tumbling_window.py')
create_and_run_glue_job('switzer-spark_streaming_tumbling_lab_example',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-05-23', '--output_table': 'billyswitzer.spark_streaming_tumbling_lab'})


