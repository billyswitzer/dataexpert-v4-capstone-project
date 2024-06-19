from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from jobs.glue_job_submission import create_glue_job
from jobs.trino_queries import run_trino_query_dq_check, execute_trino_query
from jinja2 import Environment

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


def ds_add(ds, days):
    date = datetime.strptime(ds, '%Y-%m-%d')
    date += timedelta(days=days)
    return date.strftime('%Y-%m-%d')

def add_jinja_template_function(dag):
    # Create a Jinja environment and add the custom function
    jinja_env = Environment()
    jinja_env.globals['ds_add'] = ds_add
    dag.template_undefined = jinja_env.undefined


@dag("daily_stock_price_incremental_dag",
     description="Load the previous day's stock data to staging, perform quality checks, and publish",
     default_args={
         "owner": "William Switzer",
         "start_date": datetime(2024, 6, 1),
         "retries": 0,
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
     catchup=False,
     tags=["pyspark", "glue", "eczachly", "billyswitzer"],
     template_searchpath='jobs')
def daily_stock_price_incremental_dag():
    staging_incremental_flat_table = "billyswitzer.staging_daily_stock_price_incremental"
    staging_incremental_cumulative_table = "billyswitzer.staging_daily_stock_price_cumulative"
    production_cumulative_table = "billyswitzer.daily_stock_price_cumulative"

    load_staging_incremental_flat_table = PythonOperator(
        task_id="load_staging_incremental_flat_table",
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
                "--output_table": staging_incremental_flat_table,
                "--apca_api_key_id": apca_api_key_id,
                "--apca_api_secret_key": apca_api_secret_key
            },
        },
    )

    run_dq_not_null_flat_table_check = PythonOperator(
        task_id="run_dq_not_null_flat_table_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT COUNT(CASE WHEN close_price IS NULL THEN 1 END) = 0 AS close_price_is_not_null_check,
                    COUNT(CASE WHEN high_price IS NULL THEN 1 END) = 0 AS high_price_is_not_null_check,
                    COUNT(CASE WHEN low_price IS NULL THEN 1 END) = 0 AS low_price_is_not_null_check,
                    COUNT(CASE WHEN trade_count IS NULL THEN 1 END) = 0 AS trade_count_is_not_null_check,
                    COUNT(CASE WHEN open_price IS NULL THEN 1 END) = 0 AS open_price_is_not_null_check,
                    COUNT(CASE WHEN volume IS NULL THEN 1 END) = 0 AS volume_is_not_null_check,
                    COUNT(CASE WHEN volume_weighted_average_price IS NULL THEN 1 END) = 0 AS volume_weighted_average_price_is_not_null_check,
                    COUNT(CASE WHEN symbol IS NULL THEN 1 END) = 0 AS symbol_is_not_null_check,
                    COUNT(CASE WHEN as_of_date IS NULL THEN 1 END) = 0 AS as_of_date_is_not_null_check
                FROM {staging_incremental_flat_table}
            """
        }
    )

    stage_cumulative_table_step = PythonOperator(
        task_id="stage_cumulative_table_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                    INSERT INTO {staging_incremental_cumulative_table}
                    WITH yesterday AS 
                    (
                    SELECT 
                        symbol,
                        as_of_date,
                        FILTER(price_array, x -> x.bar_date >= as_of_date - INTERVAL '1' YEAR + INTERVAL '1' DAY) AS price_array
                    FROM {production_cumulative_table}
                    WHERE as_of_date = DATE('{{{{ ds_add(ds, -2) }}}}')
                    ),
                    today AS 
                    (
                    SELECT symbol,
                        ARRAY_AGG(ROW(close_price, high_price, low_price, trade_count, open_price, bar_date, volume, volume_weighted_average_price)) AS price_array,
                        as_of_date
                    FROM {staging_incremental_flat_table}
                    WHERE close_price > 0
                        AND high_price > 0
                        AND low_price > 0
                        AND trade_count > 0
                        AND open_price > 0
                        AND volume > 0
                        AND volume_weighted_average_price > 0
                    GROUP BY symbol,
                        as_of_date
                    )
                    SELECT COALESCE(y.symbol, t.symbol) AS symbol,
                    CASE WHEN y.price_array IS NULL THEN t.price_array
                        WHEN t.price_array IS NULL THEN y.price_array
                        ELSE t.price_array || y.price_array END AS price_array,
                    DATE('{{{{ ds_add(ds, -1) }}}}') AS as_of_date
                    FROM yesterday y
                    FULL OUTER JOIN today t on y.symbol = t.symbol
                        AND y.as_of_date + INTERVAL '1' DAY = t.as_of_date
               """
        }
    )

    run_dq_new_rows_under_threshold_check = PythonOperator(
        task_id="run_dq_new_rows_under_threshold_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                WITH production_table AS 
                (
                    SELECT COUNT(1) AS row_count
                    FROM {production_cumulative_table}
                    WHERE as_of_date = DATE('{{{{ ds_add(ds, -2) }}}}')
                ),
                staging_table AS
                (
                    SELECT COUNT(1) AS row_count
                    FROM {staging_incremental_cumulative_table}
                )
                SELECT st.row_count < pt.row_count * 1.01 AS new_rows_under_threshold_check  
                FROM production_table pt
                JOIN staging_table st ON 1=1
            """
        }
    )    

    exchange_step = PythonOperator(
        task_id="exchange_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                   INSERT INTO {production_cumulative_table}
                   SELECT * FROM {staging_incremental_cumulative_table}
               """
        }
    )

    cleanup_staging_incremental_cumulative_table = PythonOperator(
        task_id="cleanup_staging_incremental_cumulative_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {staging_incremental_cumulative_table}"""
        }
    )

    #load_staging_incremental_flat_table >> \
    run_dq_not_null_flat_table_check >> \
        stage_cumulative_table_step >> \
            run_dq_new_rows_under_threshold_check >> \
                exchange_step >> \
                    cleanup_staging_incremental_cumulative_table


dag_instance = daily_stock_price_incremental_dag()

add_jinja_template_function(dag_instance)