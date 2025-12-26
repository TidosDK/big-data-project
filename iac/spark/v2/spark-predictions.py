from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import hour, col

# 1. Setup
spark = SparkSession.builder.appName("TrainEnergyModel").getOrCreate()

# 2. Load Training Data
# (Assuming you saved some data to /tmp/training_data or have a CSV)
# For this example, we create dummy data to show the structure
data = spark.createDataFrame([
    ("Elvarme", 120.5, "2024-01-01T10:00:00"),
    ("District", 80.2, "2024-01-01T11:00:00"),
    ("Elvarme", 130.1, "2024-01-01T12:00:00"),
    ("District", 85.0, "2024-01-01T13:00:00"),
], ["HeatingCategory", "ConsumptionkWh", "TimeUTC"])

# 3. Feature Engineering
# Extract Hour from Timestamp
df = data.withColumn("Hour", hour(col("TimeUTC")))

# 4. Define the ML Pipeline
# A. Convert 'HeatingCategory' string to a number index
indexer = StringIndexer(inputCol="HeatingCategory", outputCol="HeatingIndex", handleInvalid="keep")

# B. Convert index to One-Hot vector (standard for categorical data)
encoder = OneHotEncoder(inputCols=["HeatingIndex"], outputCols=["HeatingVec"])

# C. Combine features (Hour, HeatingVec) into a single 'features' vector
assembler = VectorAssembler(inputCols=["Hour", "HeatingVec"], outputCol="features")

# D. Define the Model (Linear Regression)
lr = LinearRegression(featuresCol="features", labelCol="ConsumptionkWh")

# E. Build the Pipeline
pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

# 5. Train the Model
model = pipeline.fit(df)

# 6. Save the Model to a path accessible by your Streaming Job
model_path = "/tmp/energy_prediction_model"
model.write().overwrite().save(model_path)

print(f"Model saved to {model_path}")
spark.stop()