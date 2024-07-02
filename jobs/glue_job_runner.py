from jobs.aws_secret_manager import get_secret
from jobs.glue_job_submission import create_glue_job
import os

s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
tabular_credential = get_secret("TABULAR_CREDENTIAL")
catalog_name = get_secret("CATALOG_NAME")
aws_region = get_secret("AWS_GLUE_REGION")
aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = get_secret("KAFKA_CREDENTIALS")

polygon_api_key = os.environ["POLYGON_API_KEY"]
polygon_access_key_id = os.environ["POLYGON_ACCESS_KEY_ID"]


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

#Staging Daily Stock Price Backfill
local_script_path = os.path.join("jobs", 'batch/load_staging_daily_stock_price_backfill.py')
create_and_run_glue_job('switzer-load_staging_daily_stock_price_backfill',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-06-21', \
                                   '--output_table': 'billyswitzer.staging_daily_stock_price_backfill',\
                                   '--polygon_api_key': polygon_api_key,\
                                   '--polygon_access_key_id': polygon_access_key_id
                                        })


# #Stock Splits Backfill
# local_script_path = os.path.join("jobs", 'batch/load_stock_splits_backfill.py')
# create_and_run_glue_job('switzer-load_stock_splits_backfill',
#                         script_path=local_script_path,
#                         arguments={'--ds': '2024-06-21', \
#                                    '--output_table': 'billyswitzer.stock_splits_backfill',\
#                                    '--polygon_api_key': polygon_api_key
#                                         })
