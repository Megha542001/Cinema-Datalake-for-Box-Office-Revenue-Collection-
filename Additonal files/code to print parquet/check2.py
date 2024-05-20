from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("ReadParquetData").getOrCreate()

# Read the Parquet file
parquet_file_path = "/Users/abishek/datalake/combined.parquet"
df = spark.read.parquet(parquet_file_path)

# Display the DataFrame content
df.show()
df.printSchema()

# Stop Spark Session
spark.stop()
