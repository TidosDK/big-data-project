from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StreamAndHistoric").getOrCreate()

#df = dataframe insert link to historic data
historic_df = spark.read.parquet("")

live_df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
           .option("subscribe", "live_topic")
           .load())

live_df = live_df.selectExpr("CAST(value AS STRING) as json", "timestamp")

joined_df = live_df.join(historic_df, live_df["date"] == historic_df["date"], "left")

(
    joined_df
    .selectExpr(
        "CAST(date AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "enriched_stream")
    .option("checkpointLocation", "/tmp/checkpoint/stream-and-historic")
    .start()
    .awaitTermination()
)