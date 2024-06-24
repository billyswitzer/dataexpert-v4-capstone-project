# Run from create_glue_job in DAG

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, lit, to_date, from_unixtime
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType
import requests
from datetime import datetime, timedelta
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

spark = (SparkSession.builder \
		 .config("spark.sql.caseSensitive", "true") \
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'polygon_api_key'])
run_date = args['ds']
output_table = args['output_table']
polygon_api_key = args['polygon_api_key']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

def cast_integer_value_to_float(value):
    if isinstance(value, int):
        return float(value)
    return value

#Initialize and clear staging table
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
		symbol VARCHAR(10),           
		close_price DOUBLE,
		high_price DOUBLE,
		low_price DOUBLE,
		trade_count BIGINT,
		open_price DOUBLE,
		bar_date DATE,
		volume DOUBLE,
		volume_weighted_average_price DOUBLE,
		as_of_date DATE
		)
		USING iceberg
		PARTITIONED BY (as_of_date)         
		"""
spark.sql(query)

query = f"""DELETE FROM {output_table}"""
spark.sql(query)

flat_schema = StructType([
	StructField("T", StringType(), True),
	StructField("v", DoubleType(), True),
	StructField("vw", DoubleType(), True),
	StructField("o", DoubleType(), True),
	StructField("c", DoubleType(), True),
	StructField("h", DoubleType(), True),
	StructField("l", DoubleType(), True),
	StructField("t", LongType(), True),
	StructField("n", LongType(), True)
])


run_datetime = datetime.strptime(run_date, "%Y-%m-%d")
as_of_date = run_datetime.date()
query_date = as_of_date
earliest_date = query_date - timedelta(days=365)

#Pull one year of data
while query_date > earliest_date:
	flattened_df = spark.createDataFrame(data=[], schema=flat_schema)

	url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{query_date}?adjusted=true&apiKey={polygon_api_key}"

	response = requests.get(url)
	status_code = response.status_code
	if status_code != 200:
		raise Exception(response.text)    

	data = response.json()

	if data["resultsCount"] > 0:

		results = data["results"]

		for entry in results:
			for key in entry.keys():
				if key in ['c', 'h', 'l', 'o', 'vw', 'v']:
					entry[key] = cast_integer_value_to_float(entry[key])			

		flattened_df = spark.createDataFrame(results, flat_schema)

		converted_df = flattened_df.withColumn("temp_unix_sec_timestamp", col("t") / 1000.0) \
			.withColumn("temp_timestamp", from_unixtime(col("temp_unix_sec_timestamp"))) \
			.withColumn("bar_date", to_date(col("temp_timestamp"))) \
			.select(
				col("T").alias("symbol"),
				col("c").alias("close_price"),
				col("h").alias("high_price"),
				col("l").alias("low_price"),
				col("n").alias("trade_count"),
				col("o").alias("open_price"),
				col("bar_date"),
				col("v").alias("volume"),
				col("vw").alias("volume_weighted_average_price")
			)            
		
		output_df = converted_df.withColumn('as_of_date', lit(as_of_date))

		output_df.select(
			col("symbol"),
			col("close_price"),
			col("high_price"),
			col("low_price"),
			col("trade_count"),
			col("open_price"),
			col("bar_date"),
			col("volume"),
			col("volume_weighted_average_price"),
			col("as_of_date")
		).writeTo(output_table).using("iceberg").partitionedBy("as_of_date").append()

	query_date -= timedelta(days=1)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)



