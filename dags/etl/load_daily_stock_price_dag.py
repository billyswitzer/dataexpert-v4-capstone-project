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
staging_incremental_script_path = "jobs/batch/load_staging_daily_stock_price.py"

# # #Alpaca keys
# apca_api_key_id = Variable.get("APCA_API_KEY_ID")
# apca_api_secret_key = Variable.get("APCA_API_SECRET_KEY")

#Polygon key
polygon_api_key = Variable.get("POLYGON_API_KEY")


@dag("load_daily_stock_price_dag",
     description="Load the previous day's stock data to staging, perform quality checks, and publish",
     default_args={
         "owner": "William Switzer",
         "start_date": datetime(2024, 6, 22),
         "retries": 1,
     },
     max_active_runs=1,
     schedule_interval='0 8 * * *',
     catchup=True,
     tags=["pyspark", "glue", "eczachly", "billyswitzer"],
     template_searchpath='jobs')
def load_daily_stock_price_dag():
    staging_daily_flat_table = "billyswitzer.staging_daily_stock_price"
    staging_daily_cumulative_table = "billyswitzer.staging_daily_stock_price_cumulative"
    production_cumulative_table = "billyswitzer.daily_stock_price_cumulative"
    production_dim_table = "billyswitzer.dim_daily_stock_price"

    load_staging_flat_table = PythonOperator(
        task_id="load_staging_flat_table",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "switzer-staging_daily_stock_price_job",
            "script_path": staging_incremental_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Daily load to staging table",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": staging_daily_flat_table,
                '--polygon_api_key': polygon_api_key
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
                    COUNT(CASE WHEN open_price IS NULL THEN 1 END) = 0 AS open_price_is_not_null_check,
                    COUNT(CASE WHEN volume IS NULL THEN 1 END) = 0 AS volume_is_not_null_check,
                    COUNT(CASE WHEN symbol IS NULL THEN 1 END) = 0 AS symbol_is_not_null_check,
                    COUNT(CASE WHEN as_of_date IS NULL THEN 1 END) = 0 AS as_of_date_is_not_null_check
                FROM {staging_daily_flat_table}
            """
        }
    )

    cleanup_staging_cumulative_table_pre = PythonOperator(
        task_id="cleanup_staging_cumulative_table_pre",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {staging_daily_cumulative_table}"""
        }
    )

    stage_cumulative_table_step = PythonOperator(
        task_id="stage_cumulative_table_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                    INSERT INTO {staging_daily_cumulative_table}
                    SELECT symbol,
                        ARRAY_AGG(ROW(close_price, high_price, low_price, trade_count, open_price, bar_date, volume, volume_weighted_average_price) ORDER BY bar_date DESC) AS price_array,
                        as_of_date
                    FROM {staging_daily_flat_table}
                    WHERE close_price > 0
                        AND high_price > 0
                        AND low_price > 0
                        AND trade_count > 0
                        AND open_price > 0
                        AND volume > 0
                        AND volume_weighted_average_price > 0
                    GROUP BY symbol,
                        as_of_date
               """
        }
    )

    run_dq_new_rows_under_threshold_check = PythonOperator(
        task_id="run_dq_new_rows_under_threshold_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                WITH date_cte AS 
                (
                    SELECT DATE('{{{{ ds }}}}') - INTERVAL '1' DAY AS last_partition_date
                ),            
                production_table AS 
                (
                    SELECT COUNT(1) AS row_count
                    FROM {production_cumulative_table} p
                        JOIN date_cte dc ON p.as_of_date = dc.last_partition_date
                ),
                staging_table AS
                (
                    SELECT COUNT(1) AS row_count
                    FROM {staging_daily_cumulative_table}
                )
                SELECT st.row_count < pt.row_count * 1.01 AS new_rows_under_threshold_check  
                FROM production_table pt
                    JOIN staging_table st ON 1=1
            """
        }
    )

    delete_current_partition_cumulative_prod = PythonOperator(
        task_id="delete_current_partition_cumulative_prod",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {production_cumulative_table}
                        WHERE as_of_date = DATE('{{{{ ds }}}}')"""
        }
    )        

    insert_current_partition_cumulative_prod = PythonOperator(
        task_id="insert_current_partition_cumulative_prod",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                   INSERT INTO {production_cumulative_table}
                   SELECT * FROM {staging_daily_cumulative_table}
               """
        }
    )

    cleanup_staging_incremental_cumulative_table_post = PythonOperator(
        task_id="cleanup_staging_incremental_cumulative_table_post",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {staging_daily_cumulative_table}"""
        }
    )

    delete_current_partition_dim_prod = PythonOperator(
        task_id="delete_current_partition_dim_prod",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {production_dim_table}
                        WHERE as_of_date = DATE('{{{{ ds }}}}')"""
        }
    )          

    load_dim_daily_stock_price_table = PythonOperator(
        task_id="load_dim_daily_stock_price_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_dim_table}
                WITH partition_date_cte AS 
                (
                SELECT DATE('{{{{ ds }}}}') AS current_partition_date
                ), previous_weekday AS
                (
                SELECT CASE
                        WHEN DAY_OF_WEEK(current_partition_date) = 7 THEN current_partition_date - INTERVAL '2' DAY		--Sunday
                        WHEN DAY_OF_WEEK(current_partition_date) = 6 THEN current_partition_date - INTERVAL '1' DAY		--Saturday
                        ELSE current_partition_date
                    END AS prev_weekday
                FROM partition_date_cte 
                ),
                stock_array_slice AS
                (
                SELECT dspc.symbol,
                    dspc.as_of_date,
                    FILTER(dspc.price_array, x -> x.bar_date = pw.prev_weekday) AS last_day_array,
                    FILTER(dspc.price_array, x -> x.bar_date > dspc.as_of_date - INTERVAL '90' DAY) AS last_quarter_array,
                    dspc.price_array AS last_year_array
                FROM {production_cumulative_table} dspc
                    JOIN partition_date_cte pdc ON dspc.as_of_date = pdc.current_partition_date
                    JOIN previous_weekday pw ON 1=1
                )
                SELECT symbol,
                REDUCE(last_day_array, NULL, (s, x) -> x.close_price, s -> s) AS close_price_last_day,
                REDUCE(last_quarter_array, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                    (s, x) -> CAST(ROW(s.sum + x.close_price, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
                    s -> IF(s.count = 0, NULL, s.sum / s.count)
                    ) AS close_price_avg_last_90_days,
                REDUCE(last_year_array, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                    (s, x) -> CAST(ROW(s.sum + x.close_price, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
                    s -> IF(s.count = 0, NULL, s.sum / s.count)
                    ) AS close_price_avg_last_365_days,
                as_of_date
                FROM stock_array_slice
               """
        }
    )    

    load_staging_flat_table >> \
        run_dq_not_null_flat_table_check >> \
            cleanup_staging_cumulative_table_pre >> \
                stage_cumulative_table_step >> \
                    run_dq_new_rows_under_threshold_check >> \
                        delete_current_partition_cumulative_prod >> \
                            insert_current_partition_cumulative_prod >> \
                                cleanup_staging_incremental_cumulative_table_post >> \
                                    delete_current_partition_dim_prod >> \
                                        load_dim_daily_stock_price_table


load_daily_stock_price_dag()

