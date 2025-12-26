import sys
import time
import json
import threading
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf  # Added SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


conf = SparkConf() \
    .setAppName("EnergyWeather_Spark") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.kryoserializer.buffer.max", "512m") \
    .set("spark.rdd.compress", "true")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 5)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def create_dynamic_kafka_stream(ssc, topic, group_id):
    jvm = ssc.sparkContext._gateway.jvm
    java_queue = jvm.java.util.concurrent.LinkedBlockingQueue()

    jstream = ssc._jssc.queueStream(java_queue, True)
    dstream = DStream(jstream, ssc, ssc.sparkContext.serializer)

    def producer_thread(sc, j_queue):
        print(f"Starting Consumer for {topic}...")
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='earliest',
                group_id=group_id,
                value_deserializer=lambda x: x.decode('utf-8')
            )

            #chunks manageable for Kryo
            MAX_BUFFER = 1000
            PUSH_INTERVAL = 1.0

            buffer = []
            last_push = time.time()

            for message in consumer:
                buffer.append(message.value)

                now = time.time()
                if (now - last_push > PUSH_INTERVAL) or (len(buffer) >= MAX_BUFFER):
                    if buffer:

                        while j_queue.size() > 0:
                            time.sleep(0.5)

                        rdd = sc.parallelize(buffer)
                        j_queue.add(rdd._jrdd)
                        buffer = []
                    last_push = now
        except Exception as e:
            print(f"Thread Error {topic}: {e}")

    t = threading.Thread(target=producer_thread, args=(ssc.sparkContext, java_queue))
    t.daemon = True
    t.start()

    return dstream



print("Creating Streams with Kryo...")
energy_stream = create_dynamic_kafka_stream(ssc, "energy_data", "group_energy_kryo_v1")
weather_stream = create_dynamic_kafka_stream(ssc, "meterological_observations", "group_weather_kryo_v1")


energy_windowed = energy_stream.window(60, 5)
weather_windowed = weather_stream.window(60, 5)

energy_tagged = energy_windowed.map(lambda x: ("energy", x))
weather_tagged = weather_windowed.map(lambda x: ("weather", x))

unified_stream = energy_tagged.union(weather_tagged)


def process_unified_batch(time, rdd):
    if rdd.isEmpty(): return
    print(f"========= Batch Time: {str(time)} =========")
    spark = getSparkSessionInstance(rdd.context.getConf())

    energy_schema = StructType([
        StructField("TimeUTC", StringType(), True),
        StructField("TimeDK", StringType(), True),
        StructField("HeatingCategory", StringType(), True),
        StructField("Region", StringType(), True),
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

    if energy_rdd.isEmpty():
        print("Status: Waiting for Energy data...")
        return

    try:
        df_energy = spark.read.schema(energy_schema).json(energy_rdd)
        df_weather = spark.read.schema(met_schema).json(weather_rdd)

        df_energy = df_energy.withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))

        df_weather = df_weather.select(col("properties.observed").alias("observed_time")) \
            .withColumn("event_ts_weather", to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")) \
            .withColumn("join_hour", date_trunc("hour", col("event_ts_weather")))

        joined_df = df_energy.join(df_weather, on="join_hour", how="left")

        output_df = joined_df.select(
            col("TimeUTC").alias("key"),
            to_json(struct("*")).alias("value")
        )

        output_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "processed_data") \
            .save()
        print(f"Batch written via Kryo. Count: {output_df.count()}")
    except Exception as e:
        print(f"Error processing batch: {e}")


unified_stream.foreachRDD(process_unified_batch)
ssc.start()
ssc.awaitTermination()