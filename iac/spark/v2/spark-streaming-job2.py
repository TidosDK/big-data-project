import sys
import time
import json
import threading
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

sc = SparkContext(appName="EnergyWeather_DynamicQueue")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 5)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


# --- THE FIX: DYNAMIC JAVA QUEUE ---
def create_dynamic_kafka_stream(ssc, topic, group_id):
    # 1. Create a Java LinkedBlockingQueue via Py4J
    jvm = ssc.sparkContext._gateway.jvm
    java_queue = jvm.java.util.concurrent.LinkedBlockingQueue()

    # 2. Create the DStream from the Java Queue
    # oneAtATime=False allows processing all RDDs currently in the queue
    jstream = ssc._jssc.queueStream(java_queue, False)

    # 3. Wrap in Python DStream so we can use .map(), .window()
    dstream = DStream(jstream, ssc, ssc.sparkContext.serializer)

    # 4. Producer Thread: Python Kafka -> Java Queue
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
            buffer = []
            last_push = time.time()
            for message in consumer:
                buffer.append(message.value)

                # Push every 1 second or 1000 items
                now = time.time()
                if (now - last_push > 1.0) or (len(buffer) > 1000):
                    if buffer:
                        while j_queue.size() < 5:
                            time.sleep(5)
                            # A. Create Python RDD
                        rdd = sc.parallelize(buffer)
                        # B. Convert to Java RDD and push to Java Queue
                        j_queue.add(rdd._jrdd)
                        buffer = []
                    last_push = now
        except Exception as e:
            print(f"Thread Error {topic}: {e}")

    t = threading.Thread(target=producer_thread, args=(ssc.sparkContext, java_queue))
    t.daemon = True
    t.start()

    return dstream


print("Creating Energy Stream...")
energy_stream = create_dynamic_kafka_stream(ssc, "energy_data", "queue_group_energy_v5")

print("Creating Weather Stream...")
weather_stream = create_dynamic_kafka_stream(ssc, "meterological_observations", "queue_group_weather_v5")

# --- PROCESSING LOGIC ---
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
        StructField("ConsumptionkWh", DoubleType(), True)
    ])
    met_schema = StructType([
        StructField("properties", StructType([
            StructField("observed", StringType(), True)
        ]))
    ])

    energy_rdd = rdd.filter(lambda x: x[0] == "energy").map(lambda x: x[1])
    weather_rdd = rdd.filter(lambda x: x[0] == "weather").map(lambda x: x[1])

    if energy_rdd.isEmpty():
        print("No Energy data in this window.")
        return

    try:
        df_energy = spark.read.schema(energy_schema).json(energy_rdd)
        df_weather = spark.read.schema(met_schema).json(weather_rdd)

        df_energy = df_energy.withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))

        # Weather parsing
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
        print(f"Batch written to Kafka. Rows: {output_df.count()}")
    except Exception as e:
        print(f"Error processing batch: {e}")


unified_stream.foreachRDD(process_unified_batch)
ssc.start()
ssc.awaitTermination()