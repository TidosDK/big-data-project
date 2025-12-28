import sys
import time
import json
import os
import shutil
import threading
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# --- PATHS ---
# Spark watches this folder for new files (Buffer)
STREAM_DIR = "/data/stream_buffer"
TEMP_DIR = "/data/stream_temp"

sys.path.insert(0, "/tmp/pyspark_lib")

# Clean start
try:
    os.makedirs(STREAM_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    # Clear old files so we don't re-process garbage
    os.system(f"rm -rf {STREAM_DIR}/*")
except:
    pass

conf = SparkConf() \
    .setAppName("EnergyWeather_FileStream") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# 10 Second Batch Interval
ssc = StreamingContext(sc, 10)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


prediction_model = None


def get_model():
    global prediction_model
    if prediction_model is None:
        try:
            prediction_model = PipelineModel.load("/data/energy_prediction_model")
        except:
            pass
    return prediction_model


# --- PRODUCER THREAD (Writes Files to Disk) ---
def kafka_to_file_producer():
    print("Starting Kafka-to-File Producer...", flush=True)
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        # Change Group ID to force re-read
        group_id='grp_legacy_file_FIX_v1',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    consumer.subscribe(['energy_data', 'meterological_observations'])

    buffer = []
    MAX_BUFFER = 10
    last_flush = time.time()

    while True:
        msg_dict = consumer.poll(timeout_ms=500)
        if msg_dict:
            for tp, messages in msg_dict.items():
                for m in messages:
                    # Wrap in JSON with topic name so we can filter later
                    wrapper = {"topic": m.topic, "payload": m.value}
                    buffer.append(json.dumps(wrapper))

        now = time.time()
        # Flush if buffer full OR if 2 seconds passed
        if len(buffer) > 0 and (len(buffer) >= MAX_BUFFER or (now - last_flush) > 2.0):
            timestamp = int(now * 1000)
            filename = f"batch_{timestamp}.json"
            temp_path = os.path.join(TEMP_DIR, filename)
            final_path = os.path.join(STREAM_DIR, filename)

            try:
                # Write to temp first
                with open(temp_path, 'w') as f:
                    for record in buffer:
                        f.write(record + "\n")

                # Atomic Move (Spark sees it instantly)
                shutil.move(temp_path, final_path)
                print(f"DEBUG: Wrote file {filename} with {len(buffer)} records", flush=True)
                buffer = []
                last_flush = now
            except Exception as e:
                print(f"File Write Error: {e}", flush=True)


# Start the Producer in background
t = threading.Thread(target=kafka_to_file_producer)
t.daemon = True
t.start()

# --- SPARK DSTREAM (Watches the Directory) ---
# This is standard Legacy Streaming API, but safer than QueueStream
text_dstream = ssc.textFileStream(STREAM_DIR)


def process_batch(time, rdd):
    # Always print batch time to prove Scheduler is alive
    print(f"========= Batch Time: {str(time)} =========", flush=True)

    if rdd.isEmpty():
        return

    spark = getSparkSessionInstance(rdd.context.getConf())

    try:
        # 1. Parse the Wrapper JSON (Topic + Payload)
        raw_df = spark.read.json(rdd)

        # Safety check for empty columns
        if "topic" not in raw_df.columns or "payload" not in raw_df.columns:
            return

        # 2. Split by Topic
        energy_df_raw = raw_df.filter("topic = 'energy_data'").select("payload")
        weather_df_raw = raw_df.filter("topic = 'meterological_observations'").select("payload")

        e_cnt = energy_df_raw.count()
        w_cnt = weather_df_raw.count()
        print(f"DEBUG: Counts -> Energy: {e_cnt}, Weather: {w_cnt}", flush=True)

        # 3. Define Schemas
        energy_schema = StructType([
            StructField("TimeUTC", StringType(), True),
            StructField("RegionName", StringType(), True),
            StructField("ConsumptionkWh", DoubleType(), True)
        ])
        met_schema = StructType([
            StructField("properties", StructType([
                StructField("observed", StringType(), True),
                StructField("parameterId", StringType(), True),
                StructField("value", DoubleType(), True)
            ]))
        ])

        # 4. Parse Payloads
        df_energy = None
        df_weather = None

        if e_cnt > 0:
            df_energy = spark.read.schema(energy_schema).json(energy_df_raw.rdd.map(lambda x: x.payload))
            df_energy = df_energy \
                .withColumnRenamed("RegionName", "Region") \
                .withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))

        if w_cnt > 0:
            df_weather_temp = spark.read.schema(met_schema).json(weather_df_raw.rdd.map(lambda x: x.payload))
            df_weather = df_weather_temp.select(
                col("properties.observed").alias("observed_time"),
                col("properties.parameterId").alias("parameterId"),
                col("properties.value").alias("WeatherValue")
            ).filter(col("parameterId").isin(["temp_dew", "temp_dry"]))

            df_weather = df_weather \
                .withColumn("event_ts_weather", to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_weather")))
        else:
            df_weather = spark.createDataFrame([], StructType([
                StructField("join_hour", StringType(), True),
                StructField("parameterId", StringType(), True),
                StructField("WeatherValue", DoubleType(), True)
            ]))

        # 5. Join & Predict
        if df_energy is not None:
            joined_df = df_energy.join(df_weather, on="join_hour", how="left")

            model = get_model()
            output_df = None
            try:
                if model:
                    features_df = joined_df.withColumn("Hour", hour(col("event_ts_energy")))
                    output_df = model.transform(features_df).select(
                        col("TimeUTC").alias("key"),
                        to_json(struct(
                            col("TimeUTC"), col("Region"), col("parameterId"),
                            col("WeatherValue"), col("ConsumptionkWh").alias("Actual"),
                            col("prediction").alias("Predicted")
                        )).alias("value")
                    )
                else:
                    output_df = joined_df.select(col("TimeUTC").alias("key"), to_json(struct("*")).alias("value"))
            except:
                output_df = joined_df.select(col("TimeUTC").alias("key"), to_json(struct("*")).alias("value"))

            # Write
            print("DEBUG: Writing batch to Kafka...", flush=True)
            output_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("topic", "processed_data") \
                .save()
            print(f"SUCCESS: Batch written. Rows: {output_df.count()}", flush=True)
    except Exception as e:
        print(f"Batch Error: {e}", flush=True)


text_dstream.foreachRDD(process_batch)
ssc.start()
ssc.awaitTermination()