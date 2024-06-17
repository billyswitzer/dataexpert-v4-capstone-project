# From ~/DataExpertV4/capstone-project-billy-switzer/jobs, run:
#
# ~/EcZachlySelfPaced/bootcamp3/infrastructure/4-apache-spark-training/spark-3.5.0-bin-hadoop3/bin/spark-submit \
#  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,software.amazon.awssdk:bundle:2.20.18 \
# daily_stock_price_staging_backfill.py

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col, lit, explode, to_date, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, ArrayType
import os
import requests
from datetime import date, timedelta

def main():

    if not os.environ['DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL'] \
            or not os.environ['DATA_ENGINEER_IO_WAREHOUSE'] \
                or not os.environ['APCA_API_KEY_ID'] \
                    or not os.environ['APCA_API_SECRET_KEY']:
        raise ValueError("""You need to set environment variables:
                    DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL, 
                    DATA_ENGINEER_IO_WAREHOUSE,
                    APCA_API_KEY_ID,
                    APCA_API_SECRET_KEY to run this PySpark job!
        """)      

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
    
    #Initialize and clear staging table
    output_table = 'billyswitzer.staging_daily_stock_price_backfill'
    query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
            symbol VARCHAR(10),           
            close_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            trade_count BIGINT,
            open_price DOUBLE,
            bar_date DATE,
            volume BIGINT,
            volume_weighted_average_price DOUBLE,
            as_of_date DATE
            )
            USING iceberg
            PARTITIONED BY (as_of_date)         
            """
    spark.sql(query)

    query = f"""DELETE FROM {output_table}"""
    spark.sql(query)


    as_of_date = date.today()
    
    bar_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price_array", ArrayType(
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
        ), True)
    ])

    flat_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("c", DoubleType(), True),
        StructField("h", DoubleType(), True),
        StructField("l", DoubleType(), True),
        StructField("n", LongType(), True),
        StructField("o", DoubleType(), True),
        StructField("t", StringType(), True),
        StructField("v", LongType(), True),
        StructField("vw", DoubleType(), True)
    ])

    flattened_df = spark.createDataFrame(data=[], schema=flat_schema)

    asset_json = get_assets()

    asset_df = spark.createDataFrame(asset_json)

    yesterday = date.today() - timedelta(days=1)
    one_year_ago = yesterday - timedelta(days=366)

    #Pull a batch of stocks and build the URL below dynamically
    symbol_batch_size = 10
    num_symbol_batches = asset_df.count() // symbol_batch_size + 1
    splits = [1.0/num_symbol_batches] * num_symbol_batches
    symbol_batches = asset_df.randomSplit(splits)

    headers = {
        'APCA-API-KEY-ID': os.environ['APCA_API_KEY_ID'],
        'APCA-API-SECRET-KEY': os.environ['APCA_API_SECRET_KEY'],
        'accept': 'application/json'
    }    

    for symbol_batch_df in symbol_batches:
        #Get batch of stock symbols separated by %2C
        symbol_string = '%2C'.join(symbol_batch_df.filter(~symbol_batch_df["symbol"].contains("/")).select("symbol").rdd.flatMap(lambda x: x).collect())

        initial_url = f'https://data.alpaca.markets/v2/stocks/bars?symbols={symbol_string}&timeframe=1Day&start={str(one_year_ago)}&end={str(yesterday)}&limit=1000&adjustment=raw&feed=sip&sort=desc'

        #Always run the loop the first time
        status_code = 200
        next_page_token = 'Initial'
        
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
            for symbol in data["bars"]:
                for entry in data["bars"][symbol]:
                    for key in entry:
                        if key in ['c', 'h', 'l', 'o', 'vw']:
                            entry[key] = cast_integer_value_to_float(entry[key])

            rows = [Row(symbol=symbol, price_array=bars) for symbol, bars in data["bars"].items()]
            raw_df = spark.createDataFrame(rows, bar_schema)

            #Flatten dataset so we can perform quality checks prior to loading into cumulative table
            flattened_df = flattened_df.union(raw_df.select(col("symbol"), explode(col("price_array")).alias("bar")) \
                .select(
                    col("symbol"),
                    col("bar.c").alias("c"),
                    col("bar.h").alias("h"),
                    col("bar.l").alias("l"),
                    col("bar.n").alias("n"),
                    col("bar.o").alias("o"),
                    col("bar.t").alias("t"),
                    col("bar.v").alias("v"),
                    col("bar.vw").alias("vw")
                )
            )
        #End while

        converted_df = flattened_df.withColumn("temp_timestamp", to_timestamp(col("t"), "yyyy-MM-dd'T'HH:mm:ssX")) \
            .withColumn("bar_date", to_date(col("temp_timestamp"))) \
            .select(
                col("symbol"),
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
    #End for

    spark.stop()


def get_assets():
    url = 'https://paper-api.alpaca.markets/v2/assets?status=active&attributes='
    headers = {
        'APCA-API-KEY-ID': os.environ['APCA_API_KEY_ID'],
        'APCA-API-SECRET-KEY': os.environ['APCA_API_SECRET_KEY'],
        'accept': 'application/json'
    }

    response = requests.get(url, headers=headers)

    return response.json()


def cast_integer_value_to_float(value):
    if isinstance(value, int):
        return float(value)
    return value


if __name__ == "__main__":
    main()



