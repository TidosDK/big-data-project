import sys
import time
import threading
import gc
import os

from kafka import KafkaConsumer

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_trunc, to_json, struct, hour, lead
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression

# SPARK CONFIGURATION
conf = SparkConf() \
    .setAppName("EnergyWeather_Final_Stable") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.kryoserializer.buffer.max", "1024m") \
    .set("spark.driver.maxResultSize", "2g") \
    .set("spark.cleaner.ttl", "3600") \
    .set("spark.cleaner.periodicGC.interval", "1min")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Batch Interval is set: 10 seconds
ssc = StreamingContext(sc, 10)
ssc.checkpoint("/data/checkpoints")


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


# PREDICTION MODEL
prediction_model = None


def get_model():
    global prediction_model
    model_path = "/data/energy_prediction_model"

    if prediction_model is None:
        try:
            # TRY TO LOAD EXISTING MODEL
            prediction_model = PipelineModel.load(model_path)
        except:
            print(">>> Model not found on disk. Training a fresh fallback model in-memory...")

            # FALLBACK TO DUMMY DATA
            spark = getSparkSessionInstance(None)
            data = spark.createDataFrame([
                ("2024-01-01T09:00:00", "Elvarme", 100.0),
                ("2024-01-01T10:00:00", "Elvarme", 120.0),
                ("2024-01-01T11:00:00", "Elvarme", 115.0),
                ("2024-01-01T12:00:00", "Elvarme", 130.0),
                ("2024-01-01T09:00:00", "District", 200.0)
            ], ["TimeUTC", "HeatingCategory", "ConsumptionkWh"])

            # CREATING 'NextConsumption' LABEL FOR FORECASTING
            windowSpec = Window.partitionBy("HeatingCategory").orderBy("TimeUTC")
            df = data.withColumn("NextConsumption", lead("ConsumptionkWh", 1).over(windowSpec)).na.drop()
            df = df.withColumn("Hour", hour(col("TimeUTC")))

            # PIPELINE
            indexer = StringIndexer(inputCol="HeatingCategory", outputCol="HeatingIndex", handleInvalid="keep")
            encoder = OneHotEncoder(inputCols=["HeatingIndex"], outputCols=["HeatingVec"])
            assembler = VectorAssembler(inputCols=["Hour", "HeatingVec", "ConsumptionkWh"], outputCol="features")
            lr = LinearRegression(featuresCol="features", labelCol="NextConsumption")

            pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])
            model = pipeline.fit(df)

            # SAVE + CACHE THE MODEL
            try:
                model.write().overwrite().save(model_path)
            except Exception as e:
                print(f"Warning: Could not save model to disk (ephemeral storage?): {e}")

            prediction_model = model
            print(">>> Fallback Model Trained and Loaded.")

    return prediction_model



def create_dynamic_kafka_stream(ssc, topic, group_id):
    # Access the JVM via Py4J
    jvm = ssc.sparkContext._gateway.jvm
    # Create a Java Queue (Spark Streaming natively understands this)
    java_queue = jvm.java.util.concurrent.LinkedBlockingQueue()

    # Create DStream connected to that queue
    # oneAtATime=True ensures Spark only processes ONE RDD per batch interval
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

            #Small chunks prevent Driver OOM
            MAX_BUFFER = 500
            buffer = []

            for message in consumer:
                buffer.append(message.value)

                if len(buffer) >= MAX_BUFFER:
                    # If the queue has items, Spark is busy. Wait.
                    # This prevents the Python thread from flooding the Java Heap.
                    while j_queue.size() > 0:
                        time.sleep(0.5)

                    try:
                        rdd = sc.parallelize(buffer)
                        j_queue.add(rdd._jrdd)
                        buffer = []
                        del rdd
                    except Exception as e:
                        print(f"Push Error: {e}")
                        time.sleep(1)
        except Exception as e:
            print(f"Thread Error {topic}: {e}")

    t = threading.Thread(target=producer_thread, args=(ssc.sparkContext, java_queue))
    t.daemon = True
    t.start()

    return dstream

print("Initializing Streams...")
# Unique group IDs ensure we don't conflict with old consumers
energy_stream = create_dynamic_kafka_stream(ssc, "energy_data", "grp_energy_final_v1")
weather_stream = create_dynamic_kafka_stream(ssc, "meterological_observations", "grp_weather_final_v1")

# Windowing must match or exceed batch interval
energy_windowed = energy_stream.window(60, 10)
weather_windowed = weather_stream.window(60, 10)

# TAGGING STREAMS FOR UNION
energy_tagged = energy_windowed.map(lambda x: ("energy", x))
weather_tagged = weather_windowed.map(lambda x: ("weather", x))

unified_stream = energy_tagged.union(weather_tagged)

def process_unified_batch(time, rdd):
    # Force GC to clear metadata from previous batch
    gc.collect()

    if rdd.isEmpty(): return

    print(f"========= Batch Time: {str(time)} =========")
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

    # Separate Data
    energy_rdd = rdd.filter(lambda x: x[0] == "energy").map(lambda x: x[1])
    weather_rdd = rdd.filter(lambda x: x[0] == "weather").map(lambda x: x[1])

    if energy_rdd.isEmpty():
        print("Status: Waiting for Energy data...")

    try:
        if not energy_rdd.isEmpty():
            df_energy = spark.read.schema(energy_schema).json(energy_rdd)
            df_energy = df_energy \
                .withColumnRenamed("RegionName", "Region") \
                .withColumn("event_ts_energy", to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_energy")))
        else:
            return  # No energy data to process


        if not weather_rdd.isEmpty():
            df_weather_raw = spark.read.schema(met_schema).json(weather_rdd)

            # Flatten & Select
            df_weather = df_weather_raw.select(
                col("properties.observed").alias("observed_time"),
                col("properties.parameterId").alias("parameterId"),
                col("properties.value").alias("WeatherValue")
            )

            # FILTER FOR TEMP_DEV + TEMP_DRY
            df_weather = df_weather.filter(col("parameterId").isin(["temp_dew", "temp_dry"]))

            df_weather = df_weather \
                .withColumn("event_ts_weather", to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")) \
                .withColumn("join_hour", date_trunc("hour", col("event_ts_weather")))
        else:
            # DUMMY DATAFRAME FOR FILLING NULL IN
            df_weather = spark.createDataFrame([], StructType([
                StructField("join_hour", to_timestamp(col("TimeUTC")).dataType, True),
                StructField("parameterId", StringType(), True),
                StructField("WeatherValue", DoubleType(), True)
            ]))

        # C. JOIN (Left Join on Time broadcasts weather to all regions)
        joined_df = df_energy.join(df_weather, on="join_hour", how="left")

        # D. PREDICT & FORMAT
        model = get_model()

        if model:
            try:
                # Prepare features for model (Hour + HeatingCategory)
                features_df = joined_df.withColumn("Hour", hour(col("event_ts_energy")))

                # Run Prediction
                result_df = model.transform(features_df)

                # Format Output JSON
                output_df = result_df.select(
                    col("TimeUTC").alias("key"),
                    to_json(struct(
                        col("TimeUTC"),
                        col("Region"),
                        col("parameterId"),
                        col("WeatherValue"),
                        col("ConsumptionkWh").alias("Actual"),
                        col("prediction").alias("Predicted")
                    )).alias("value")
                )
            except Exception as e:
                print(f"Prediction error (fallback used): {e}")
                # Fallback Output (No prediction)
                output_df = joined_df.select(
                    col("TimeUTC").alias("key"),
                    to_json(struct(
                        col("TimeUTC"), col("Region"), col("parameterId"),
                        col("WeatherValue"), col("ConsumptionkWh").alias("Actual")
                    )).alias("value")
                )
        else:
            # No model loaded
            output_df = joined_df.select(
                col("TimeUTC").alias("key"),
                to_json(struct(
                    col("TimeUTC"), col("Region"), col("parameterId"),
                    col("WeatherValue"), col("ConsumptionkWh").alias("Actual")
                )).alias("value")
            )

        # E. WRITE TO KAFKA
        output_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "processed_data") \
            .save()

        print(f"Batch successfully written. Count: {output_df.count()}")

    except Exception as e:
        print(f"Critical error processing batch: {e}")


unified_stream.foreachRDD(process_unified_batch)

# Start context
ssc.start()
ssc.awaitTermination()