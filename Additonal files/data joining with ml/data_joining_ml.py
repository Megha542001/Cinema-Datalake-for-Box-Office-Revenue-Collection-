from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create SparkSession
spark = SparkSession.builder.appName("DataJoiningAndML").getOrCreate()

# Read the joined data from parquet files
joined_df = spark.read.parquet("/Users/abishek/datalake/joined_data.parquet")

# Check if the joined dataset is empty
if joined_df.count() == 0:
    print("Error: Joined dataset is empty.")
    spark.stop()
    exit(1)

# Define the feature columns and label column
feature_cols = ["popularity", "vote_average"]
label_col = "revenue"

# Create a VectorAssembler to combine the feature columns into a single vector column
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Transform the joined dataset to include the feature vector column
transformed_df = assembler.transform(joined_df)

# Split the dataset into training and testing sets
(training_data, testing_data) = transformed_df.randomSplit([0.7, 0.3])

# Create a LinearRegression model
lr = LinearRegression(featuresCol="features", labelCol=label_col)

# Check if the training dataset is empty
if training_data.count() == 0:
    print("Error: Training dataset is empty.")
    spark.stop()
    exit(1)

# Fit the linear regression model on the training dataset
model = lr.fit(training_data)

# Make predictions on the testing dataset
predictions = model.transform(testing_data)

# Show the predicted values
predictions.select("features", "label", "prediction").show()

# Stop the SparkSession
spark.stop()
