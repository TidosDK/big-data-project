import sys
import time
import threading
import gc
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

conf = SparkConf() \
    .setAppName("EnergyWeather_job") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.kryoserializer.buffer.max", "1024m") \
    .set("spark.driver.maxResultSize", "2g") \
    .set("spark.cleaner.ttl", "3600")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")  # Less noise, more signal

ssc = StreamingContext(sc, 10)

#ssc.checkpoint("/data/checkpoints_fresh_v100")

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


# --- DYNAMIC QUEUE WITH TIME FLUSH ---
def create_dynamic_kafka_stream(ssc, topic, group_id):
    jvm = ssc.sparkContext._gateway.jvm
    java_queue = jvm.java.util.concurrent.LinkedBlockingQueue()

    jstream = ssc._jssc.queueStream(java_queue, False)
    dstream = DStream(jstream, ssc, ssc.sparkContext.serializer)

    def producer_thread(sc, j_queue):
        print(f"Starting Consumer for {topic}...", flush=True)
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='earliest',
                group_id=group_id,
                value_deserializer=lambda x: x.decode('utf-8'),
                consumer_timeout_ms=1000  # Timeout allows loop to cycle even if no new messages
            )

            MAX_BUFFER = 10
            buffer = []
            last_push_time = time.time()  # Track time

            # Using an infinite loop to ensure we check time even if kafka is quiet
            while True:
                # Poll allows us to get messages or return empty list after timeout
                msg_dict = consumer.poll(timeout_ms=500)

                if msg_dict:
                    for topic_partition, messages in msg_dict.items():
                        for message in messages:
                            buffer.append(message.value)

                # --- THE FIX: TIME OR SIZE ---
                current_time = time.time()
                time_diff = current_time - last_push_time

                should_push = (len(buffer) >= MAX_BUFFER) or (len(buffer) > 0 and time_diff > 1.0)

                if should_push:
                    # Wait for Spark to clear queue (Backpressure)
                    while j_queue.size() > 0:
                        time.sleep(0.5)

                    try:
                        print(f"DEBUG: Pushing {len(buffer)} records from {topic} (Time diff: {time_diff:.2f}s)",
                              flush=True)
                        rdd = sc.parallelize(buffer)
                        j_queue.add(rdd._jrdd)
                        buffer = []
                        last_push_time = current_time  # Reset timer
                        del rdd
                    except Exception as e:
                        time.sleep(1)

        except Exception as e:
            print(f"Thread Error {topic}: {e}", flush=True)

    t = threading.Thread(target=producer_thread, args=(ssc.sparkContext, java_queue))
    t.daemon = True
    t.start()
    return dstream


print("Creating Streams...", flush=True)

energy_stream = create_dynamic_kafka_stream(
    ssc, "energy_data", "grp_energy_debug_RETRY_v7"
)
weather_stream = create_dynamic_kafka_stream(
    ssc, "meterological_observations", "grp_weather_debug_RETRY_v7"
)
# --- PROCESSING ---
energy_windowed = energy_stream.window(60, 10)
weather_windowed = weather_stream.window(60, 10)

energy_tagged = energy_windowed.map(lambda x: ("energy", x))
weather_tagged = weather_windowed.map(lambda x: ("weather", x))

unified_stream = energy_tagged.union(weather_tagged)


def process_unified_batch(time, rdd):
    gc.collect()
    if rdd.isEmpty():
        return  # Don't print for empty batches to keep logs clean

    # Flush ensures logs appear immediately in K8s
    print(f"========= Batch Time: {str(time)} =========", flush=True)
    spark = getSparkSessionInstance(rdd.context.getConf())

    energy_schema = StructType([
        StructField("TimeUTC", StringType(), True),
        StructField("TimeDK", StringType(), True),
        StructField("RegionName", StringType(), True),
        StructField("HeatingCategory", StringType(), True),
        StructField("ConsumptionkWh", DoubleType(), True)
    ])

    met_schema = StructType([
        StructField("properties", StructType([
            StructField("observed", StringType(), True),
            StructField("parameterId", StringType(), True),
            StructField("value", DoubleType(), True)
        ]))
    ])

    energy_rdd = rdd.filter(lambda x: x[0] == "energy").map(lambda x: x[1])
    weather_rdd = rdd.filter(lambda x: x[0] == "weather").map(lambda x: x[1])

    # Debug Prints
    print(f"DEBUG: Counts -> Energy: {energy_rdd.count()}, Weather: {weather_rdd.count()}", flush=True)

    if energy_rdd.isEmpty():
        print("Status: Waiting for Energy data...", flush=True)

    try:
        df_energy = None

        if not energy_rdd.isEmpty():
            df_energy = spark.read.schema(energy_schema).json(energy_rdd)
            df_energy = df_energy \
                .withColumnRenamed("RegionName", "Region") \
                .withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))

        if not weather_rdd.isEmpty():
            df_weather_raw = spark.read.schema(met_schema).json(weather_rdd)
            df_weather = df_weather_raw.select(
                col("properties.observed").alias("observed_time"),
                col("properties.parameterId").alias("parameterId"),
                col("properties.value").alias("WeatherValue")
            ).filter(col("parameterId").isin(["temp_dew", "temp_dry"]))

            df_weather = df_weather \
                .withColumn("event_ts_weather", to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_weather")))
        else:
            df_weather = spark.createDataFrame([], StructType([
                StructField("join_hour", to_timestamp(col("TimeUTC")).dataType, True),
                StructField("parameterId", StringType(), True),
                StructField("WeatherValue", DoubleType(), True)
            ]))

        if df_energy is not None:
            joined_df = df_energy.join(df_weather, on="join_hour", how="left")

            model = get_model()
            if model:
                try:
                    features_df = joined_df.withColumn("Hour", hour(col("event_ts_energy")))
                    output_df = model.transform(features_df).select(
                        col("TimeUTC").alias("key"),
                        to_json(struct(
                            col("TimeUTC"), col("Region"), col("parameterId"),
                            col("WeatherValue"), col("ConsumptionkWh").alias("Actual"),
                            col("prediction").alias("Predicted")
                        )).alias("value")
                    )
                except:
                    output_df = joined_df.select(col("TimeUTC").alias("key"), to_json(struct("*")).alias("value"))
            else:
                output_df = joined_df.select(col("TimeUTC").alias("key"), to_json(struct("*")).alias("value"))

            output_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("topic", "processed_data") \
                .save()
            print(f"SUCCESS: Batch written. Rows: {output_df.count()}", flush=True)

    except Exception as e:
        print(f"Error processing batch: {e}", flush=True)


unified_stream.foreachRDD(process_unified_batch)
ssc.start()
ssc.awaitTermination()