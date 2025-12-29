import sys
import os

sys.path.insert(0, "/tmp/pyspark_lib")

import time
import json
import shutil
import threading
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
# ADDED: coalesce, lit (For handling missing values)
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct, hour, dayofmonth, month, year, \
    dayofweek, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# --- PATHS ---
STREAM_DIR = "/data/stream_buffer"
TEMP_DIR = "/data/stream_temp"
try:
    os.makedirs(STREAM_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.system(f"rm -rf {STREAM_DIR}/*")
except:
    pass

conf = SparkConf().setAppName("EnergyWeather_Fix").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# INCREASED BATCH INTERVAL to 20s to catch matching data
ssc = StreamingContext(sc, 20)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


prediction_model = None


def get_model():
    global prediction_model
    if prediction_model is None:
        try:
            prediction_model = PipelineModel.load("/data/energy_prediction_model")
            print("DEBUG: Model loaded.", flush=True)
        except:
            print("WARN: Model NOT found.", flush=True)
    return prediction_model


# --- PRODUCER THREAD ---
def kafka_to_file_producer():
    print("Starting Producer...", flush=True)
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            # New group to re-read data
            group_id='grp_join_fix_v3',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        consumer.subscribe(['energy_data', 'meterological_observations'])

        buffer = []
        # INCREASED BUFFER: Wait for more data to overlap
        MAX_BUFFER = 500
        last_flush = time.time()

        while True:
            msg_dict = consumer.poll(timeout_ms=500)
            if msg_dict:
                for tp, messages in msg_dict.items():
                    for m in messages:
                        wrapper = {"topic": m.topic, "payload": m.value}
                        buffer.append(json.dumps(wrapper))

            now = time.time()
            # Wait up to 10 seconds to collect matching energy/weather
            if len(buffer) > 0 and (len(buffer) >= MAX_BUFFER or (now - last_flush) > 10.0):
                timestamp = int(now * 1000)
                filename = f"batch_{timestamp}.json"
                temp_path = os.path.join(TEMP_DIR, filename)
                final_path = os.path.join(STREAM_DIR, filename)
                try:
                    with open(temp_path, 'w') as f:
                        for record in buffer: f.write(record + "\n")
                    shutil.move(temp_path, final_path)
                    print(f"DEBUG: Flushed {len(buffer)} records (Buffer Window: 10s)", flush=True)
                    buffer = []
                    last_flush = now
                except:
                    pass
    except:
        pass


t = threading.Thread(target=kafka_to_file_producer)
t.daemon = True
t.start()

# --- PROCESSING ---
text_dstream = ssc.textFileStream(STREAM_DIR)


def process_batch(time, rdd):
    print(f"=== Batch: {str(time)} ===", flush=True)
    if rdd.isEmpty(): return

    spark = getSparkSessionInstance(rdd.context.getConf())
    try:
        raw_df = spark.read.json(rdd)
        if "topic" not in raw_df.columns: return

        energy_df_raw = raw_df.filter("topic = 'energy_data'").select("payload")
        weather_df_raw = raw_df.filter("topic = 'meterological_observations'").select("payload")

        print(f"DEBUG: Counts -> Energy: {energy_df_raw.count()}, Weather: {weather_df_raw.count()}", flush=True)

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

        df_energy = None
        df_weather = None

        if not energy_df_raw.isEmpty():
            df_energy = spark.read.schema(energy_schema).json(energy_df_raw.rdd.map(lambda x: x.payload))
            df_energy = df_energy.withColumnRenamed("RegionName", "Region") \
                .withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))

        if not weather_df_raw.isEmpty():
            df_weather_temp = spark.read.schema(met_schema).json(weather_df_raw.rdd.map(lambda x: x.payload))
            df_weather = df_weather_temp.select(
                col("properties.observed").alias("observed_time"),
                col("properties.parameterId").alias("parameterId"),
                col("properties.value").alias("WeatherValue")
            ).filter(col("parameterId").isin(["temp_dew", "temp_dry"])) \
                .withColumn("event_ts_weather", to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_weather")))
        else:
            df_weather = spark.createDataFrame([], StructType([
                StructField("join_hour", StringType(), True),
                StructField("parameterId", StringType(), True),
                StructField("WeatherValue", DoubleType(), True)
            ]))

        if df_energy is not None:
            # JOIN
            joined_df = df_energy.join(df_weather, on="join_hour", how="left")

            model = get_model()
            output_df = None

            if model:
                try:
                    # Prepare Features
                    features_df = joined_df \
                        .withColumn("Hour", hour(col("event_ts_energy")).cast(DoubleType())) \
                        .withColumn("Day", dayofmonth(col("event_ts_energy")).cast(DoubleType())) \
                        .withColumn("Month", month(col("event_ts_energy")).cast(DoubleType())) \
                        .withColumn("Year", year(col("event_ts_energy")).cast(DoubleType())) \
                        .withColumn("DayOfWeek", dayofweek(col("event_ts_energy")).cast(DoubleType())) \
                        .na.fill(0)  # Fills WeatherValue=0 if null

                    predictions = model.transform(features_df)

                    # OUTPUT SELECTION
                    output_df = predictions.select(
                        col("TimeUTC").alias("key"),
                        to_json(struct(
                            col("TimeUTC"),
                            col("Region"),
                            # FIX: Show 'MISSING' if null so we know why
                            coalesce(col("parameterId"), lit("MISSING_WEATHER")).alias("parameterId"),
                            col("WeatherValue"),
                            col("ConsumptionkWh").alias("Actual"),
                            col("prediction").alias("Predicted")
                        )).alias("value")
                    )
                except Exception as e:
                    print(f"PREDICTION ERROR: {e}", flush=True)
                    output_df = joined_df.select(col("TimeUTC").alias("key"), to_json(struct("*")).alias("value"))
            else:
                output_df = joined_df.select(col("TimeUTC").alias("key"), to_json(struct("*")).alias("value"))

            output_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("topic", "processed_data") \
                .save()
            print(f"SUCCESS: Written {output_df.count()} rows", flush=True)

    except Exception as e:
        print(f"Batch Failed: {e}", flush=True)


text_dstream.foreachRDD(process_batch)
ssc.start()
ssc.awaitTermination()