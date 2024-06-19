# Run from create_glue_job in DAG

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col, lit, explode, to_date, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, ArrayType
import requests
from datetime import datetime, timedelta
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'source_table', 'target_table', 'postgres_username', \
                                     'postgres_password','postgres_url','postgres_port','postgres_database'])
run_date = args['ds']
source_table = args['source_table']
target_table = args['target_table']
postgres_username = args['postgres_username']
postgres_password = args['postgres_password']
postgres_url = args['postgres_url']
postgres_port = args['postgres_port']
postgres_database = args['postgres_database']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

jdbc_url = f"jdbc:postgresql://{postgres_url}:{postgres_port}/{postgres_database}"
properties = {
    "user": f"{postgres_username}",
    "password": f"{postgres_password}",
    "driver": f"org.postgresql.Driver"
}

run_datetime = datetime.strptime(run_date, "%Y-%m-%d")
as_of_date = run_datetime.date() - timedelta(days=1)

# Postgres table will be created dynamically via overwrite to ensure all fields are present and data is up-to-date
# Use the Iceberg table to define the requisite columns
dim_daily_stock_price_df = spark.sql(f"""SELECT symbol,
		close_price_last_day,
		close_price_avg_last_90_days,
		close_price_avg_last_365_days,
		CAST(NULL AS DOUBLE) AS current_price,
		CAST(NULL AS DOUBLE) AS m_price_change_last_day,
		CAST(NULL AS DOUBLE) AS m_price_change_last_day_pct,
		CAST(NULL AS DOUBLE) AS m_price_change_last_90_days,
		CAST(NULL AS DOUBLE) AS m_price_change_last_90_days_pct,
		CAST(NULL AS DOUBLE) AS m_price_change_last_365_days,
		CAST(NULL AS DOUBLE) AS m_price_change_last_365_days_pct,
		CAST(NULL AS TIMESTAMP) AS last_updated_datetime
	FROM {source_table}
    WHERE as_of_date = DATE('{str(as_of_date)}')""")

dim_daily_stock_price_df.write.jdbc(url=jdbc_url, table=target_table, mode="overwrite", properties=properties)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)



