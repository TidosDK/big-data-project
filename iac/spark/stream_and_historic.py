import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, date_trunc, to_json, struct
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

driver_host = os.getenv("SPARK_DRIVER_HOST", "127.0.0.1")

spark = SparkSession.builder \
    .appName("EnergyWeather") \
    .master("spark://spark-master-0.spark-headless.bd-bd-gr-02.svc.cluster.local:7077") \
    .config("spark.driver.bindAddress", driver_host) \
    .config("spark.driver.host", driver_host) \
    .getOrCreate()

#spark = SparkSession.builder.appName("EnergyWeatherJoinTimeOnly").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

# Schemas (only time fields necessary)
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

# --- energy_data stream ---
energy_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "energy_data")
    .option("startingOffsets", "earliest")
    .load()
)

energy_df = (
    energy_raw_df
    .select(
        from_json(col("value").cast("string"), energy_schema).alias("data"),
        col("timestamp").alias("kafka_ts")
    )
    .select("data.*", "kafka_ts")
)

energy_df = energy_df.withWatermark("event_ts_energy", "2 hours")

energy_df = energy_df.withColumn(
    "event_ts_energy",
    to_timestamp(col("TimeUTC"), "yyyy-MM-dd'T'HH:mm:ss")
)
energy_df = energy_df.withColumn(
    "join_hour",
    date_trunc("hour", col("event_ts_energy"))
)

# --- meterological_observations stream ---
meterological_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "meterological_observations")
    .option("startingOffsets", "earliest")
    .load()
)

meterological_df = (
    meterological_raw_df
    .select(
        from_json(col("value").cast("string"), met_schema).alias("data"),
        col("timestamp").alias("kafka_ts")
    )
)

meterological_df = meterological_df.select(
    col("data.properties.observed").alias("observed_time"),
    col("kafka_ts")
)

meterological_df = meterological_df.withWatermark("event_ts_weather", "2 hours")

meterological_df = meterological_df.withColumn(
    "event_ts_weather",
    to_timestamp(col("observed_time"), "yyyy-MM-dd'T'HH:mm:ssX")  # handles 'Z'
)
meterological_df = meterological_df.withColumn(
    "join_hour",
    date_trunc("hour", col("event_ts_weather"))
)

# No watermarks yet, keep it simple
joined = energy_df.join(
    meterological_df,
    on="join_hour",
    how="left"
)

# Debug joined output
console_query = (
    joined
    .select("TimeUTC", "observed_time", "join_hour")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

value_struct = struct(*[col(c) for c in joined.columns])

output_df = (
    joined
    .select(
        col("TimeUTC").alias("key"),
        to_json(value_struct).alias("value")
    )
)



kafka_query = (
    output_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "processed_data")
    .option("checkpointLocation", "/tmp/checkpoints/energy-weather-join-time-only")
    .outputMode("append")
    .start()
)

kafka_query.awaitTermination()
