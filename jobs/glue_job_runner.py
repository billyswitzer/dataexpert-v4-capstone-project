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

#Alpaca keys
apca_api_key_id = os.environ["APCA_API_KEY_ID"]
apca_api_secret_key = os.environ["APCA_API_SECRET_KEY"]

#Postgres keys
postgres_username = os.environ["POSTGRES_USERNAME"]
postgres_password = os.environ["POSTGRES_PASSWORD"]
postgres_url = os.environ["POSTGRES_URL"]
postgres_port = os.environ["POSTGRES_PORT"]
postgres_database = os.environ["POSTGRES_DATABASE"]


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



local_script_path = os.path.join("jobs", 'batch/load_dim_to_postgres_current_daily_stock_price.py')
create_and_run_glue_job('switzer-load_dim_to_postgres_current_daily_stock_price',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-06-17', \
                                   '--source_table': 'billyswitzer.dim_daily_stock_price',\
                                   '--target_table': 'billyswitzer.current_day_stock_price',\
                                    # '--apca_api_key_id': apca_api_key_id,\
                                    # '--apca_api_secret_key': apca_api_secret_key,\
                                    '--postgres_username': postgres_username,\
                                    '--postgres_password': postgres_password,\
                                    '--postgres_url': postgres_url,\
                                    '--postgres_port': postgres_port,\
                                    '--postgres_database': postgres_database,\
                                        })


