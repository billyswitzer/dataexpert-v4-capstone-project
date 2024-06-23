from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from jobs.glue_job_submission import create_glue_job
from jobs.trino_queries import run_trino_query_dq_check, execute_trino_query

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
initial_load_to_current_day_stock_price = "jobs/batch/load_dim_daily_stock_price_to_current_day_stock_price.py"
update_current_day_stock_price_microbatch_script = "jobs/batch/update_current_day_stock_price_microbatch.py"

# #Alpaca keys
apca_api_key_id = Variable.get("APCA_API_KEY_ID")
apca_api_secret_key = Variable.get("APCA_API_SECRET_KEY")




@dag("update_current_day_stock_price_microbatch_dag",
     description="Load the most recent dim_daily_stock_price to current_day_stock_price and run microbatch to update current_day_stock_price throughout the day",
     default_args={
         "owner": "William Switzer",
         "start_date": datetime(2024, 6, 21),
         "retries": 1,
     },
     max_active_runs=1,
     schedule_interval='55 13 * * 1-5',
     catchup=False,
     tags=["pyspark", "glue", "eczachly", "billyswitzer"],
     template_searchpath='jobs')
def update_current_day_stock_price_microbatch_dag():
    source_table = "billyswitzer.dim_daily_stock_price"
    target_table = "billyswitzer.current_day_stock_price"

    load_dim_daily_stock_price_to_current_day_stock_price = PythonOperator(
        task_id="load_dim_daily_stock_price_to_current_day_stock_price",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "switzer_load_dim_daily_stock_price_to_current_day_stock_price",
            "script_path": initial_load_to_current_day_stock_price,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Incremental Load to staging table",
            "arguments": {
                "--ds": "{{ ds }}",
                "--source_table": source_table,
                "--target_table": target_table
            },
        },
    )

    update_current_day_stock_price_microbatch = PythonOperator(
        task_id="update_current_day_stock_price_microbatch",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "switzer_update_current_day_stock_price_microbatch",
            "script_path": update_current_day_stock_price_microbatch_script,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Incremental Load to staging table",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": target_table,
                "--apca_api_key_id": apca_api_key_id,
                "--apca_api_secret_key": apca_api_secret_key,
                "--runtime_minutes": "10"           #10 for testing, should be 7.5 hours = 450 minutes for production to account for daylight savings time
            },
        },
    )    


    load_dim_daily_stock_price_to_current_day_stock_price >> \
        update_current_day_stock_price_microbatch 


update_current_day_stock_price_microbatch_dag()

