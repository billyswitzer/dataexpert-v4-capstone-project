# From ~/DataExpertV4/capstone-project-billy-switzer/jobs, run:
#
# ~/EcZachlySelfPaced/bootcamp3/infrastructure/4-apache-spark-training/spark-3.5.0-bin-hadoop3/bin/spark-submit \
#  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,software.amazon.awssdk:bundle:2.20.18 \
# local/load_dim_ticker_details.py


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, isnan
import pandas as pd
import requests

run_date = '2024-06-26'
output_table = 'billyswitzer.dim_ticker_details'
polygon_api_key = os.environ["POLYGON_API_KEY"]

def get_ticker_details(ticker):
    response = requests.get(f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={polygon_api_key}")
    if response.status_code == 200:
        return response.json()
    else:
        return None


def main():

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

    tickers = spark.sql(f"""SELECT ticker FROM billyswitzer.daily_stock_price_cumulative
        WHERE as_of_date = DATE('{run_date}')
        """).rdd.flatMap(lambda x: x).collect()
    
    results_list = []
    for ticker in tickers:
         result = get_ticker_details(ticker)
         if result:
              results_list.append(result["results"])

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        ticker VARCHAR(10),
        ticker_root VARCHAR(10),
        company_name VARCHAR(50),
        is_actively_traded BOOLEAN,
        currency_name VARCHAR(25),
        country VARCHAR(50),
        asset_type VARCHAR(25),
        market_cap DOUBLE,
        primary_exchange VARCHAR(25),
        industry_description VARCHAR(100),
        weighted_shares_outstanding DOUBLE,
        market_cap_description VARCHAR(25),
        as_of_date DATE
    )
    USING iceberg
    PARTITIONED BY (as_of_date)
    """)

    results_df = pd.DataFrame(results_list)
    selected_columns = ["ticker","ticker_root","name","active","currency_name","locale", "market", "market_cap", "primary_exchange","sic_description","weighted_shares_outstanding"]

    output_df = spark.createDataFrame(results_df[selected_columns])
    
    output_df.withColumn('as_of_date', lit(run_date).cast("date")) \
        .withColumn("market_cap_description",
                    when(isnan(col("market_cap")) | col("market_cap").isNull(), "Not Provided")
                   .when(col("market_cap") > 200000000000, "Mega Cap")
                   .when(col("market_cap") > 10000000000, "Large Cap")
                   .when(col("market_cap") > 2000000000, "Mid Cap")
                   .when(col("market_cap") > 250000000, "Small Cap")
                   .otherwise("Micro Cap")) \
        .select(
            col("ticker"),
            col("ticker_root"),
            col("name").alias("company_name"),
            col("active").alias("is_actively_traded"),
            col("currency_name"),
            col("locale").alias("country"),
            col("market").alias("asset_type"),
            col("market_cap"),
            col("primary_exchange"),
            col("sic_description").alias("industry_description"),
            col("weighted_shares_outstanding"),
            col("market_cap_description"),
            col("as_of_date")
        ) \
            .sortWithinPartitions(col("asset_type"), col("market_cap_description")).writeTo(output_table) \
            .tableProperty("write.spark.fanout.enabled", "true") \
            .overwritePartitions()

    spark.stop()

if __name__ == "__main__":
    main()