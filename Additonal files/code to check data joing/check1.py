from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the TMDB and OMDB parquet files
tmdb_df = spark.read.parquet("/Users/abishek/datalake/formatted/tmdb.parquet")
omdb_df = spark.read.parquet("/Users/abishek/datalake/formatted/omdb.parquet")

# Perform the join on the common column "omdb_imdb_id" and "id"
joined_df = tmdb_df.join(omdb_df, tmdb_df.id == omdb_df.omdb_imdb_id, "inner")

# Show the joined dataset
joined_df.show()
