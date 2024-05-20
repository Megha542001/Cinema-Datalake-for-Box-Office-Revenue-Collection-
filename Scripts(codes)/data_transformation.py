from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
from pyspark.sql.types import StringType

# Create Spark Session
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# Load TMDB JSON data
tmdb_json_file_path = "/Users/abishek/datalake/tmdb_movie.json"
tmdb_schema = spark.read.json(tmdb_json_file_path).schema
tmdb_df = spark.read.schema(tmdb_schema).json(tmdb_json_file_path)

# Load OMDB JSON data
omdb_json_file_path = "/Users/abishek/datalake/omdb_movie.json"
omdb_schema = spark.read.json(omdb_json_file_path).schema
omdb_df = spark.read.schema(omdb_schema).json(omdb_json_file_path)

# Formatting and transformation for TMDB data
tmdb_df = tmdb_df.withColumn("release_date", to_timestamp(col("release_date"), "yyyy-MM-dd"))
tmdb_df = tmdb_df.withColumn("release_date", col("release_date").cast("timestamp"))
tmdb_df = tmdb_df.withColumn("release_date", col("release_date").cast("long"))
tmdb_df = tmdb_df.withColumn("release_date_utc", col("release_date") * 1000)  # Convert to UTC timestamp

# Formatting and transformation for OMDB data
omdb_df = omdb_df.withColumn("Year", col("Year").cast("integer"))

# Normalize all other column values
tmdb_columns = tmdb_df.columns
omdb_columns = omdb_df.columns

for column in tmdb_columns:
    tmdb_df = tmdb_df.withColumn(column, when(col(column).isNull(), "").otherwise(col(column).cast(StringType())))

for column in omdb_columns:
    omdb_df = omdb_df.withColumn(column, when(col(column).isNull(), "").otherwise(col(column).cast(StringType())))

# Select the required columns, including imdb_id
tmdb_df = tmdb_df.select("id", "adult", "backdrop_path", "genre_ids", "imdb_id", "original_language", "original_title",
                         "overview", "popularity", "poster_path", "release_date", "title", "video", "vote_average",
                         "vote_count", "release_date_utc")

# Write the cleaned data to Parquet files
output_path = "/Users/abishek/datalake/formatted"
tmdb_df.write.mode("overwrite").parquet(output_path + "/tmdb.parquet")
omdb_df.write.mode("overwrite").parquet(output_path + "/omdb.parquet")

# Stop Spark Session
spark.stop()
