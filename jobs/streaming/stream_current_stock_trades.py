import websocket
import threading


import ast
import sys
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json, udf, to_timestamp, window, max_
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType, DoubleType, ArrayType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime



#import websocket
#import threading
import json
import time
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import from_json, col
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Define the WebSocket client functions
def on_message(ws, message):
    print(f"Received: {message}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    def run(*args):
        authentication_message = {
            "action": "auth",
            "key": "PKCAYIDF7QMQY8LSWKNB",
            "secret": "xYiVlB43W7AcQpwXnnjMdnthHVHfXarDdyGMf24I"
        }
        ws.send(json.dumps(authentication_message))
        time.sleep(1)
        subscription_message = {
            "action": "subscribe",
            "trades": ["*"]
        }
        ws.send(json.dumps(subscription_message))
        print("Sent subscription message")
        while True:
            time.sleep(1)
        ws.close()
        print("Thread terminating...")
    thread = threading.Thread(target=run)
    thread.start()

# Start the WebSocket client in a separate thread
def start_websocket_client():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://stream.data.alpaca.markets/v2/sip",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

# Start the WebSocket client
ws_thread = threading.Thread(target=start_websocket_client)
ws_thread.start()

# Give the WebSocket client some time to establish connection and subscribe
time.sleep(5)

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("WebSocketStream") \
#     .getOrCreate()

# # Define the schema for JSON data
# schema = StructType([
#     StructField("field1", StringType(), True),
#     StructField("field2", IntegerType(), True)
# ])

# # Define the socket source
# host = "wss://stream.data.alpaca.markets/v2/sip"  # Replace with your host
# #port = 9999         # Replace with your port

# # Read from the socket
# socketDF = spark.readStream \
#     .format("socket") \
#     .option("host", host) \
#     .load()

# # Parse the JSON data
# parsedDF = socketDF.select(from_json(col("value"), schema).alias("parsed_value"))

# # Select the fields you need
# selectedDF = parsedDF.select("parsed_value.*")

# # Process the data (in this case, just print to the console)
# query = selectedDF.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# #query.awaitTermination()






spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'target_table', 'postgres_username', \
                                     'postgres_password','postgres_url','postgres_port','postgres_database'])

print(args)
run_date = args['ds']
target_table = args['target_table']
postgres_username = args['postgres_username']
postgres_password = args['postgres_password']
postgres_url = args['postgres_url']
postgres_port = args['postgres_port']
postgres_database = args['postgres_database']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session





# # Retrieve Kafka credentials from environment variables
# kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
# kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
# kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
# kafka_topic = kafka_credentials['KAFKA_TOPIC']

# if kafka_key is None or kafka_secret is None:
#     raise ValueError("KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables.")

# Kafka configuration

start_timestamp = f"{run_date}T00:00:00.000Z"


# # Define the schema for JSON data
# schema = StructType([
#     StructField("field1", StringType(), True),
#     StructField("field2", IntegerType(), True)
# ])

# Define the socket source
host = "wss://stream.data.alpaca.markets/v2/sip"  # Replace with your host
#port = 9999         # Replace with your port

# # Read from the socket
# socketDF = spark.readStream \
#     .format("socket") \
#     .option("host", host) \
#     .load()



# # Define the schema of the Kafka message value
# schema = StructType([
#     StructField("url", StringType(), True),
#     StructField("referrer", StringType(), True),
#     StructField("user_agent", StructType([
#         StructField("family", StringType(), True),
#         StructField("major", StringType(), True),
#         StructField("minor", StringType(), True),
#         StructField("patch", StringType(), True),
#         StructField("device", StructType([
#             StructField("family", StringType(), True),
#             StructField("major", StringType(), True),
#             StructField("minor", StringType(), True),
#             StructField("patch", StringType(), True),
#         ]), True),
#         StructField("os", StructType([
#             StructField("family", StringType(), True),
#             StructField("major", StringType(), True),
#             StructField("minor", StringType(), True),
#             StructField("patch", StringType(), True),
#         ]), True)
#     ]), True),
#     StructField("headers", MapType(keyType=StringType(), valueType=StringType()), True),
#     StructField("host", StringType(), True),
#     StructField("ip", StringType(), True),
#     StructField("event_time", TimestampType(), True)
# ])


# Define the schema of the Kafka message value
schema = StructType([
    StructField("T", StringType(), True),       #message type, always “t”
    StructField("s", StringType(), True),       #symbol
    StructField("i", IntegerType(), True),      #trade ID
    StructField("x", StringType(), True),       #exchange code where the trade occurred
    StructField("p", DoubleType(), True),       #trade price
    StructField("s", StringType(), True),       #trade size
    StructField("c", ArrayType(), True),        #trade condition
    StructField("t", StringType(), True),       #RFC-3339 formatted timestamp with nanosecond precision
    StructField("z", StringType(), True)        #tape
])


# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {output_table} (
#   url STRING,
#   referrer STRING,
#   user_agent STRUCT<
#     family: STRING,
#     major: STRING,
#     minor: STRING,
#     patch: STRING,
#     device: STRUCT<
#       family: STRING,
#       major: STRING,
#       minor: STRING,
#       patch: STRING
#     >,
#     os: STRUCT<
#       family: STRING,
#       major: STRING,
#       minor: STRING,
#       patch: STRING
#     >
#   >,
#   headers MAP<STRING, STRING>,
#   host STRING,
#   ip STRING,
#   event_time TIMESTAMP,
#   instructor_name STRING,
#   follower_count BIGINT,
#   offers_bootcamp BOOLEAN
# )
# USING ICEBERG
# PARTITIONED BY (hours(event_time))
# """)

# # Read from Kafka in batch mode
# kafka_df = (spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", kafka_topic) \
#     .option("startingOffsets", "earliest") \
#     .option("maxOffsetsPerTrigger", 10000) \
#     .option("kafka.security.protocol", "SASL_SSL") \
#     .option("kafka.sasl.mechanism", "PLAIN") \
#     .option("kafka.sasl.jaas.config",
#             f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
#     .load()
# )

# Read from the socket
socket_df = spark.readStream \
    .format("socket") \
    .option("host", host) \
    .load()

def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())



stock_streaming_input_df = socket_df \
    .withColumn("symbol",decode_udf(col("s"))) \
    .withColumn("trade_price",col("p")) \
    .withColumn("trade_size",col("s")) \
    .withColumn("timestamp",to_timestamp(col("s"),"yyyy-MM-dd'T'HH:mm:ss'Z'"))# \
    #.withWatermark("timestamp", "5 seconds")

windowed_df = stock_streaming_input_df.groupBy(window(col("timestamp"), "1 minute"),
                                        col("symbol")) \
                                        .agg(max_("timestamp")).alias("max_timestamp")


result_df = windowed_df.join(stock_streaming_input_df, (windowed_df["symbol"] == stock_streaming_input_df["symbol"]) & (windowed_df["max_timestamp"] == stock_streaming_input_df["timestamp"])) \
    .select(stock_streaming_input_df["symbol"],
            stock_streaming_input_df["trade_price"].alias("current_price"),
            stock_streaming_input_df["trade_size"],
            stock_streaming_input_df["timestamp"].alias("last_updated_datetime"))


jdbc_url = f"jdbc:postgresql://{postgres_url}:{postgres_port}/{postgres_database}"
jdbc_properties = {
    "user": f"{postgres_username}",
    "password": f"{postgres_password}",
    "driver": f"org.postgresql.Driver"
}

# def update_postgres_table(batch_df):
#     # Function to update PostgreSQL table
#     batch_df.write \
#         .jdbc(url=jdbc_url, table="billyswitzer.current_day_stock_price", mode="append", properties=jdbc_properties)

def update_postgres_table(batch_df, batch_id):
    # Function to upsert PostgreSQL table
    batch_df.createOrReplaceTempView("batch_updates")

    # Use Spark to execute SQL query for upsert
    update_query = """
        MERGE INTO billyswitzer.current_day_stock_price AS target
        USING batch_updates AS source
        ON target.symbol = source.symbol
        WHEN MATCHED THEN
            UPDATE SET target.current_price = source.current_price,
                       target.last_updated_datetime = source.last_updated_datetime
    """
    
    spark.sql(update_query).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "billyswitzer.current_day_stock_price") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .mode("append") \
        .save()

query = result_df.writeStream \
    .outputMode("update") \
    .foreachBatch(update_postgres_table) \
    .start()



# reference_df = spark.sql("SELECT * FROM zachwilson.host_reference_table")
#last_refresh = datetime.now()

# def insert_new_kafka_data(df, batch_id):
#     global reference_df
#     global last_refresh
#     now = datetime.now()
#     td = now - last_refresh
#     td_mins = int(round(td.total_seconds() / 60))

#     # Only refresh the reference data every 30 minutes
#     if td_mins >= 30:
#         reference_df = spark.sql("SELECT * FROM zachwilson.host_reference_table")
#         last_refresh = datetime.now()
#     # Extract the value from Kafka messages and cast to String
#     new_df = df.select("key", "value", "topic") \
#         .withColumn("decoded_value", decode_udf(col("value"))) \
#         .withColumn("value", from_json(col("decoded_value"), schema)) \
#         .select("value.*") \
#         .join(reference_df, reference_df["hostname"] == col("host"), "left") \
#         .select("url",
#                 "referrer",
#                 "user_agent",
#                 "headers",
#                 "host",
#                 "ip",
#                 "event_time",
#                 "instructor_name",
#                 "follower_count",
#                 "offers_bootcamp"
#                 )
#     view_name = "kafka_source_" + str(batch_id)
#     new_df.createOrReplaceGlobalTempView(view_name)
#     spark.sql(f"""
#     MERGE INTO {output_table} AS t
#         USING global_temp.{view_name} AS s
#          ON t.ip = s.ip AND t.event_time = s.event_time
#         WHEN NOT MATCHED THEN INSERT *
#     """)



# query = kafka_df \
#     .writeStream \
#     .format("iceberg") \
#     .outputMode("append") \
#     .trigger(processingTime="30 seconds") \
#     .foreachBatch(insert_new_kafka_data) \
#     .option("checkpointLocation", checkpoint_location) \
#     .start()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# stop the job after 5 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*1)


