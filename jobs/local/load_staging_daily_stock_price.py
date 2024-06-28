# From ~/DataExpertV4/capstone-project-billy-switzer/jobs, run:
#
# ~/EcZachlySelfPaced/bootcamp3/infrastructure/4-apache-spark-training/spark-3.5.0-bin-hadoop3/bin/spark-submit \
#  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,software.amazon.awssdk:bundle:2.20.18 \
# load_staging_daily_stock_price.py


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, lit, col

run_date = '2024-06-24'
output_table = 'billyswitzer.staging_daily_stock_price'
polygon_api_key = os.environ["POLYGON_API_KEY"]
polygon_access_key_id = os.environ["POLYGON_ACCESS_KEY_ID"]

# Initialize SparkConf and SparkContext - run locally
spark = SparkSession.builder \
    .appName("PySparkSQLReadFromTable") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", os.environ['DATA_ENGINEER_IO_WAREHOUSE']) \
    .config("spark.sql.catalog.eczachly-academy-warehouse",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.eczachly-academy-warehouse.catalog-impl",
            "org.apache.iceberg.rest.RESTCatalog") \
    .config("spark.sql.catalog.eczachly-academy-warehouse.uri",
            "https://api.tabular.io/ws/") \
    .config("spark.sql.catalog.eczachly-academy-warehouse.credential",
            os.environ['DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL']) \
    .config("spark.sql.catalog.eczachly-academy-warehouse.warehouse",
            os.environ['DATA_ENGINEER_IO_WAREHOUSE']) \
    .getOrCreate()

# Define the S3 bucket and access configuration
s3_bucket = "s3a://flatfiles"
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", polygon_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", polygon_api_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://files.polygon.io/")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} (
    ticker STRING,	
    volume BIGINT,
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    window_start BIGINT,
    transactions BIGINT,
    snapshot_date DATE
)
USING iceberg
PARTITIONED BY (snapshot_date)
""")

year = str(run_date).split('-')[0]
month = str(run_date).split('-')[1]    

file_path = f"{s3_bucket}/us_stocks_sip/day_aggs_v1/{year}/{month}/{run_date}.csv.gz"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)

df.withColumn('snapshot_date', from_unixtime(col("window_start")/lit(1000*1000*1000)).cast('date')) \
	.sortWithinPartitions("ticker").writeTo(output_table) \
	.tableProperty("write.spark.fanout.enabled", "true") \
	.overwritePartitions()
