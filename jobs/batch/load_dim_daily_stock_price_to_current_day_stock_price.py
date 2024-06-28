# Run from create_glue_job in DAG

from pyspark.sql import SparkSession
from datetime import datetime
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


spark = (SparkSession.builder \
		 .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'source_table', 'target_table', 'ticker_details_table'])
run_date = args['ds']
source_table = args['source_table']
target_table = args['target_table']
ticker_details_table = args['ticker_details_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

run_datetime = datetime.strptime(run_date, "%Y-%m-%d")
as_of_date = run_datetime.date()

dim_daily_stock_price_df = spark.sql(f"""
	WITH date_cte AS
	(
		SELECT MAX(as_of_date) AS max_as_of_date
		FROM {ticker_details_table}
	),
	current_dim_ticker_details AS
	(
		SELECT ticker,
			market_cap_description
		FROM {ticker_details_table} dtd
			JOIN date_cte dc ON dtd.as_of_date = dc.max_as_of_date
	)
	SELECT s.ticker,
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
		COALESCE(td.market_cap_description,'Not Provided') AS market_cap_description,
		CAST(NULL AS TIMESTAMP) AS last_updated_datetime
	FROM {source_table} s
	  LEFT JOIN current_dim_ticker_details td ON s.ticker = td.ticker
    WHERE as_of_date = DATE('{str(as_of_date)}')""")

dim_daily_stock_price_df.writeTo(target_table).using("iceberg").overwritePartitions()


job = Job(glueContext)
job.init(args["JOB_NAME"], args)

