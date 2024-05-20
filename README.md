# Movie-Review-DataLake


## Project Overview:

Welcome to Cinemalytics, your gateway to the Movie Review DataLake! 🎬📊

Cinemalytics is an ambitious project that delves deep into the cinematic world by curating an extensive repository of movie reviews from diverse sources. It aims to empower film enthusiasts, critics, and data aficionados with data-driven insights and comprehensive analysis.

### Architecture Highlights:

1. **Data Extraction:**
   - Creation of jobs to extract data from TMDb and OMDb APIs.
   - Focus on movies released in 2022 and 2023 to manage data volume and API limits.

2. **Data Transformation:**
   - Rigorous data cleaning, formatting, and handling of missing values.
   - Leverage of Apache Spark for processing and conversion to Parquet files.

3. **Data Combination:**
   - Unification of data from TMDb and OMDb sources using Spark.
   - Generation of a consolidated Parquet file, 'combined.parquet'.

4. **Data Indexing & Visualization:**
   - Design and creation of an Elasticsearch index based on the data schema.
   - Implementation of Kibana visualizations offering insights into movie popularity, release years, average ratings, video availability, and more.

5. **Cloud Storage Integration:**
   - Integration of Google Cloud Storage for efficient file management.
   - Implementation of DAGs facilitating file uploads to the cloud bucket.

## Project Structure:

### Directory Breakdown:

- **Data Extraction:** Jobs fetching data from TMDb and OMDb APIs.
- **Data Transformation:** Scripts for meticulous data cleaning and formatting.
- **Data Combination:** Codebase merging and creating a unified dataset.
- **Data Indexing & Visualization:** Elasticsearch schema and Kibana visualization details.
- **Cloud Storage Integration:** DAG setup and code for Google Cloud Storage interaction.

## Getting Started:

Get ready to dive into the cinematic realm:

1. **Clone this Repository:** Begin your journey by cloning this repository to your local machine.
2. **Setup Instructions:** Find detailed setup instructions within respective directories.
3. **Data Transformation & Analysis:** Follow the outlined steps in the documentation for transformation, combination, and indexing.


