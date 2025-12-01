from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StreamAndHistoric").getOrCreate()

# df = dataframe insert link to historic data
meterological_observations_df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
                                 .option("subscribe", "meterological_observations")
                                 .load())
# meterological_observations
energy_df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
             .option("subscribe", "energy_data")
             .load())

energy_df = energy_df.selectExpr("CAST(value AS STRING) as json", "timestamp")

joined_df = energy_df.join(meterological_observations_df, energy_df["date"] == meterological_observations_df["date"],
                           "left")
print(energy_df)
print(joined_df)
query = (
    joined_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "processed_data")
    .option("checkpointLocation", "/tmp/checkpoints/processed")
    .start()
)

query.awaitTermination()
print(query)
(
    joined_df
    .selectExpr(
        "CAST(date AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "processed_data")
    .option("checkpointLocation", "/tmp/checkpoint/stream-and-historic")
    .start()
    .awaitTermination()
)
print(joined_df)
