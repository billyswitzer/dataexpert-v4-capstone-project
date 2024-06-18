from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from jobs.glue_job_submission import create_glue_job
# import pendulum

# local_tz = pendulum.timezone('America/New_York')

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
script_path = "jobs/batch/daily_stock_price_staging_incremental.py"

# #Alpaca keys
apca_api_key_id = Variable.get("APCA_API_KEY_ID")
apca_api_secret_key = Variable.get("APCA_API_SECRET_KEY")


@dag("daily_stock_price_incremental_dag",
     description="Load the previous day's stock data to staging, perform quality checks, and publish",
     default_args={
         "owner": "William Switzer",
         "start_date": datetime(2024, 6, 1),
         "retries": 1,
     },
     max_active_runs=1,
     # This is a cron interval shortcut
     # * * * * *
     # Shortcuts are
     # @daily
     # @hourly
     # @monthly
     # @yearly
     #schedule_interval="@daily",
     schedule_interval='0 8 * * *',
     #timezone=local_tz,
     catchup=False,
     tags=["pyspark", "glue", "eczachly", "billyswitzer"],
     template_searchpath='jobs')
def daily_stock_price_incremental_dag():
    # default_output_table = "billyswitzer.prod_table_staging_{{ ds_nodash }}"
    # production_table = "billyswitzer.prod_table_example"
    default_output_table = "billyswitzer.staging_daily_stock_price_incremental"

    load_staging_table = PythonOperator(
        task_id="run_glue_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "switzer_daily_stock_price_incremental_job",
            "script_path": script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Incremental Load to staging table",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": default_output_table,
                "--apca_api_key_id": apca_api_key_id,
                "--apca_api_secret_key": apca_api_secret_key
            },
        },
    )

    # run_dq_check = PythonOperator(
    #     task_id="run_dq_check",
    #     python_callable=run_trino_query_dq_check,
    #     op_kwargs={
    #         'query': f"""
    #             SELECT 
    #                 date,
    #                 COUNT(CASE WHEN some_column IS NULL THEN 1 END) = 0 as column_is_not_null_check,
    #                 COUNT(1) > 0 AS is_there_data_check
    #             FROM {default_output_table}
    #             GROUP BY date
    #         """
    #     }
    # )

    # exchange_step = PythonOperator(
    #     task_id="exchange_step",
    #     python_callable=execute_trino_query,
    #     op_kwargs={
    #         'query': f"""
    #                INSERT INTO {production_table}
    #                SELECT * FROM {default_output_table}
    #            """
    #     }
    # )

    # cleanup_step = PythonOperator(
    #     task_id="cleanup_step",
    #     python_callable=execute_trino_query,
    #     op_kwargs={
    #         'query': f"""DELETE FROM {default_output_table}"""
    #     }
    # )

    load_staging_table #>> run_dq_check >> exchange_step >> cleanup_step


daily_stock_price_incremental_dag()
