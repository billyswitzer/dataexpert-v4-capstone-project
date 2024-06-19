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

# #Alpaca keys
apca_api_key_id = os.environ["APCA_API_KEY_ID"]
apca_api_secret_key = os.environ["APCA_API_SECRET_KEY"]


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



local_script_path = os.path.join("jobs", 'batch/daily_stock_price_staging_incremental.py')
create_and_run_glue_job('switzer-staging_daily_stock_price_incremental',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-06-18', \
                                   '--output_table': 'billyswitzer.staging_daily_stock_price_incremental',\
                                    '--apca_api_key_id': apca_api_key_id,\
                                    '--apca_api_secret_key': apca_api_secret_key})


