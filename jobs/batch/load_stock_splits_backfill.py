import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import requests
spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table','polygon_api_key'])
run_date = args['ds']
output_table = args['output_table']
polygon_api_key = args['polygon_api_key']

starter_url = f'https://api.polygon.io/v3/reference/splits?ticker.gte=A&execution_date.gte=2023-06-01&execution_date.lte={run_date}&limit=1000&apiKey={polygon_api_key}'
response = requests.get(starter_url)
stock_splits = []
data = response.json()
stock_splits.extend(data['results'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

def cast_integer_value_to_float(value):
    if isinstance(value, int):
        return float(value)
    return value

# Collect the results on the driver
while data['status'] == 'OK' and 'next_url' in data:
    response = requests.get(data['next_url'] + '&apiKey=' + polygon_api_key)
    data = response.json()
    stock_splits.extend(data['results'])

#Ensure doubles are not cast as integers
for entry in stock_splits:
    for key in entry:
        if key in ['split_from', 'split_to']:
            entry[key] = cast_integer_value_to_float(entry[key])    

as_of_date = datetime.strptime(run_date, "%Y-%m-%d").date()
stock_split_df = spark.createDataFrame(stock_splits).withColumn('as_of_date', lit(as_of_date))

spark.sql(f"""CREATE OR REPLACE TABLE {output_table} (
    execution_date string,
    split_from double,
    split_to double,
    ticker string,
    as_of_date DATE
    )
    USING iceberg 
    PARTITIONED BY (as_of_date)
""")

stock_split_df.select(
    col("execution_date"),
    col("split_from"),
    col("split_to"),
    col("ticker"),
    col("as_of_date")
).writeTo(output_table).using("iceberg").partitionedBy("as_of_date").overwritePartitions()
#stock_split_df.printSchema()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
