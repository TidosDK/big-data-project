from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import hour, col, lead
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("TrainNextValueModel").getOrCreate()

data = spark.createDataFrame([
    ("2024-01-01T09:00:00", "Elvarme", 100.0),
    ("2024-01-01T10:00:00", "Elvarme", 120.0),
    ("2024-01-01T11:00:00", "Elvarme", 115.0),
    ("2024-01-01T12:00:00", "Elvarme", 130.0),
], ["TimeUTC", "HeatingCategory", "ConsumptionkWh"])

windowSpec = Window.partitionBy("HeatingCategory").orderBy("TimeUTC")


df = data.withColumn("NextConsumption", lead("ConsumptionkWh", 1).over(windowSpec))


df = df.na.drop()

df = df.withColumn("Hour", hour(col("TimeUTC")))

indexer = StringIndexer(inputCol="HeatingCategory", outputCol="HeatingIndex")
encoder = OneHotEncoder(inputCols=["HeatingIndex"], outputCols=["HeatingVec"])
assembler = VectorAssembler(inputCols=["Hour", "HeatingVec", "ConsumptionkWh"], outputCol="features")

lr = LinearRegression(featuresCol="features", labelCol="NextConsumption")

pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

model = pipeline.fit(df)
model.write().overwrite().save("/tmp/energy_prediction_model")

print("Forecast Model Saved.")