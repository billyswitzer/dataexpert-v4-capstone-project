# Run from create_glue_job in DAG

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col, to_timestamp, round, when
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType
import requests
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import sys


spark = (SparkSession.builder \
		 .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
			.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'runtime_minutes', 'apca_api_key_id','apca_api_secret_key'])
run_date = args['ds']
output_table = args['output_table']
apca_api_key_id = args['apca_api_key_id']
apca_api_secret_key = args['apca_api_secret_key']
runtime_minutes = args['runtime_minutes']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

headers = {
    'APCA-API-KEY-ID': apca_api_key_id,
    'APCA-API-SECRET-KEY': apca_api_secret_key,
	'accept': 'application/json'
}  

def get_assets():
    url = 'https://paper-api.alpaca.markets/v2/assets?status=active&attributes='
    response = requests.get(url, headers=headers)
    return response.json()


def cast_integer_value_to_float(value):
    if isinstance(value, int):
        return float(value)
    return value

bar_schema = StructType([
	StructField("ticker", StringType(), True),
	StructField("price_struct",
		StructType([
			StructField("c", DoubleType(), True),
			StructField("h", DoubleType(), True),
			StructField("l", DoubleType(), True),
			StructField("n", LongType(), True),
			StructField("o", DoubleType(), True),
			StructField("t", StringType(), True),
			StructField("v", LongType(), True),
			StructField("vw", DoubleType(), True)
		])
	)
])

flat_schema = StructType([
	StructField("ticker", StringType(), True),
	StructField("c", DoubleType(), True),
	StructField("t", StringType(), True),
])

asset_json = get_assets()

asset_df = spark.createDataFrame(asset_json)

current_data_df = spark.sql(f"""
	SELECT ticker,
		close_price_last_day,
		close_price_avg_last_90_days,
		close_price_avg_last_365_days
	FROM {output_table}""")

start_time = datetime.now()
end_time = datetime.now() + timedelta(minutes=int(runtime_minutes))

#Continually poll the API and write resulting data until the job ends
while datetime.now() < end_time:
	#Pull a batch of stocks and build the URL below dynamically
	symbol_batch_size = 500
	num_symbol_batches = asset_df.count() // symbol_batch_size + 1
	splits = [1.0/num_symbol_batches] * num_symbol_batches
	symbol_batches = asset_df.randomSplit(splits)

	for symbol_batch_df in symbol_batches:
		#Get batch of stock symbols separated by %2C
		symbol_string = '%2C'.join(symbol_batch_df.filter(~symbol_batch_df["symbol"].contains("/")).select("symbol").rdd.flatMap(lambda x: x).collect())

		initial_url = f'https://data.alpaca.markets/v2/stocks/bars/latest?symbols={symbol_string}&feed=sip'

		#Always run the loop the first time
		status_code = 200
		next_page_token = 'Initial'

		flattened_df = spark.createDataFrame(data=[], schema=flat_schema)
		
		while status_code == 200 and next_page_token is not None:

			if next_page_token == 'Initial':
				url = initial_url
			else:
				url = initial_url + '&page_token=' + next_page_token

			response = requests.get(url, headers=headers)
			status_code = response.status_code
			if status_code != 200:
				raise Exception(response.text)    

			data = response.json()
			try:
				next_page_token = data['next_page_token']
			except:
				next_page_token = None

			#Ensure doubles are not cast as integers
			for ticker in data["bars"]:
				entry = data["bars"][ticker]
				for key in entry:
					if key in ['c', 'h', 'l', 'o', 'vw']:
						entry[key] = cast_integer_value_to_float(entry[key])

			rows = [Row(ticker=ticker, price_array=bars) for ticker, bars in data["bars"].items()]
			raw_df = spark.createDataFrame(rows, bar_schema)

			flattened_df = flattened_df.union(raw_df.select(
					col("ticker"),
					col("price_struct.c").alias("c"),
					col("price_struct.t").alias("t")
				)
			)
		#End while

		converted_df = flattened_df.filter(col("c") > 0).withColumn("timestamp", to_timestamp(col("t"), "yyyy-MM-dd'T'HH:mm:ssX")) \
			.select(
				col("ticker"),
				col("c").alias("current_price"),
				col("timestamp").alias("last_updated_datetime")
			)
		
		update_df = converted_df.join(current_data_df, converted_df.ticker == current_data_df.ticker) \
			.withColumn("m_price_change_last_day", round(col("current_price") - col("close_price_last_day"),2)) \
			.withColumn(
				"m_price_change_last_day_pct",
				when(
					(col("close_price_last_day").isNull()) | (col("close_price_last_day") == 0),
					None
				).otherwise(
					round(100 * (col("current_price") - col("close_price_last_day")) / col("close_price_last_day"), 2)
				)
			) \
			.withColumn("m_price_change_last_90_days", round(col("current_price") - col("close_price_avg_last_90_days"),2)) \
			.withColumn(
				"m_price_change_last_90_days_pct",
				when(
					(col("close_price_avg_last_90_days").isNull()) | (col("close_price_avg_last_90_days") == 0),
					None
				).otherwise(
					round(100 * (col("current_price") - col("close_price_avg_last_90_days")) / col("close_price_avg_last_90_days"), 2)
				)
			) \
			.withColumn("m_price_change_last_365_days", round(col("current_price") - col("close_price_avg_last_365_days"),2)) \
			.withColumn(
				"m_price_change_last_365_days_pct",
				when(
					(col("close_price_avg_last_365_days").isNull()) | (col("close_price_avg_last_365_days") == 0),
					None
				).otherwise(
					round(100 * (col("current_price") - col("close_price_avg_last_365_days")) / col("close_price_avg_last_365_days"), 2)
				)
			) \
			.select(
				converted_df.ticker,
				col("current_price"),
				col("last_updated_datetime"),
				col("m_price_change_last_day"),
				col("m_price_change_last_day_pct"),
				col("m_price_change_last_90_days"),
				col("m_price_change_last_90_days_pct"),
				col("m_price_change_last_365_days"),
				col("m_price_change_last_365_days_pct")
			)
		
		update_df.createOrReplaceTempView("stock_microbatch_update")

		spark.sql(f"""
			MERGE INTO {output_table} AS target
			USING stock_microbatch_update AS source
				ON source.ticker = target.ticker
			WHEN MATCHED THEN
				UPDATE SET current_price = source.current_price,
					last_updated_datetime = source.last_updated_datetime,
					m_price_change_last_day = source.m_price_change_last_day,
					m_price_change_last_day_pct = source.m_price_change_last_day_pct,
					m_price_change_last_90_days = source.m_price_change_last_90_days,
					m_price_change_last_90_days_pct = source.m_price_change_last_90_days_pct,
					m_price_change_last_365_days = source.m_price_change_last_365_days,
					m_price_change_last_365_days_pct = source.m_price_change_last_365_days_pct;
			""")	
	#End for
#End while


