from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read the cleaned TMDB and OMDB data from parquet files
tmdb_df = spark.read.parquet("/Users/abishek/datalake/formatted/tmdb.parquet")
omdb_df = spark.read.parquet("/Users/abishek/datalake/formatted/omdb.parquet")

# Perform a left outer join using imdb_id and imdbID columns
joined_df = tmdb_df.join(omdb_df, tmdb_df.imdb_id == omdb_df.imdbID, "left_outer") \
                  .select(tmdb_df["*"], omdb_df["title"].alias("omdb_title"))

# Save the joined DataFrame as a combined Parquet file
output_path = "/Users/abishek/datalake/combined.parquet"
joined_df.write.mode("overwrite").parquet(output_path)

# Stop Spark Session
spark.stop()
