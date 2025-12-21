import sys
import os

# --- 1. PATCH: EMBED MISSING KAFKA WRAPPER ---
# (Kept identical to the version that fixed the ImportError and AttributeError)
KAFKA_MODULE_PATH = "/tmp/kafka010.py"

KAFKA_010_CODE = r"""
from pyspark.streaming import DStream
from py4j.java_collections import MapConverter

class KafkaUtils(object):
    @staticmethod
    def createDirectStream(ssc, locationStrategy, consumerStrategy):
        try:
            # 1. Get the Java stream
            jstream = ssc._jvm.org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
                ssc._jssc, locationStrategy, consumerStrategy
            )
        except TypeError as e:
            if "KafkaUtils" in str(e):
                print("Error: The Spark-Kafka JARs are missing from the classpath.")
            raise e

        # 2. Get serializer from SparkContext (Fixed for Spark 3.5+)
        serializer = ssc.sparkContext.serializer

        # 3. Return the Python DStream wrapper
        return DStream(jstream, ssc, serializer)

class LocationStrategies(object):
    @staticmethod
    def PreferConsistent():
        from pyspark import SparkContext
        return SparkContext._active_spark_context._jvm.org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent()

    @staticmethod
    def PreferBrokers():
        from pyspark import SparkContext
        return SparkContext._active_spark_context._jvm.org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers()

class ConsumerStrategies(object):
    @staticmethod
    def Subscribe(topics, kafkaParams, offsets=None):
        from pyspark import SparkContext
        gateway = SparkContext._active_spark_context._gateway
        jtopics = MapConverter().convert(topics, gateway._gateway_client) if isinstance(topics, dict) else topics
        jparams = MapConverter().convert(kafkaParams, gateway._gateway_client)
        joffsets = MapConverter().convert(offsets, gateway._gateway_client) if offsets else None

        if joffsets:
            return SparkContext._active_spark_context._jvm.org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe(
                jtopics, jparams, joffsets
            )
        else:
             return SparkContext._active_spark_context._jvm.org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe(
                jtopics, jparams
            )
"""

# Write patch
with open(KAFKA_MODULE_PATH, "w") as f:
    f.write(KAFKA_010_CODE)
sys.path.insert(0, "/tmp")

# Import patch
try:
    import kafka010
    from kafka010 import KafkaUtils, LocationStrategies, ConsumerStrategies

    print("Successfully loaded patched KafkaUtils.")
except ImportError as e:
    print(f"CRITICAL ERROR: {e}")
    sys.exit(1)

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- 2. SETUP CONTEXTS ---
sc = SparkContext(appName="EnergyWeatherLegacy_SerialFix")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 5)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


# --- 3. KAFKA PARAMS ---
kafka_params = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "legacy_project_group_serial_fix",
    "auto.offset.reset": "earliest",
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
}

# --- 4. CREATE DSTREAMS ---
raw_energy_stream = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent(),
    ConsumerStrategies.Subscribe(["energy_data"], kafka_params)
)

raw_weather_stream = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent(),
    ConsumerStrategies.Subscribe(["meterological_observations"], kafka_params)
)

# --- 5. PROCESSING LOGIC (FIXED) ---

# CRITICAL FIX: Extract .value() BEFORE windowing.
# This converts the Java ConsumerRecord object into a standard Python String.
energy_strings = raw_energy_stream.map(lambda record: record.value())
weather_strings = raw_weather_stream.map(lambda record: record.value())

# Now we window the STRINGS (which are serializable)
energy_windowed = energy_strings.window(60, 5)
weather_windowed = weather_strings.window(60, 5)

# Tag them for the union
energy_tagged = energy_windowed.map(lambda val: ("energy", val))
weather_tagged = weather_windowed.map(lambda val: ("weather", val))

unified_stream = energy_tagged.union(weather_tagged)


def process_unified_batch(time, rdd):
    if rdd.isEmpty():
        return

    print(f"========= Batch Time: {str(time)} =========")
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Schemas
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

    # Filter RDDs
    energy_rdd = rdd.filter(lambda x: x[0] == "energy").map(lambda x: x[1])
    weather_rdd = rdd.filter(lambda x: x[0] == "weather").map(lambda x: x[1])

    if energy_rdd.isEmpty():
        print("No Energy data in this window.")
        return

    try:
        # Create DataFrames
        # Using RDDs of strings directly
        df_energy = spark.read.schema(energy_schema).json(energy_rdd)
        df_weather = spark.read.schema(met_schema).json(weather_rdd)

        # Transformations
        df_energy = df_energy.withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))

        df_weather = df_weather.select(col("properties.observed").alias("observed_time")) \
            .withColumn("event_ts_weather", to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")) \
            .withColumn("join_hour", date_trunc("hour", col("event_ts_weather")))

        # Join
        joined_df = df_energy.join(df_weather, on="join_hour", how="left")

        # Output to Kafka
        output_df = joined_df.select(
            col("TimeUTC").alias("key"),
            to_json(struct("*")).alias("value")
        )

        # Write to Kafka
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