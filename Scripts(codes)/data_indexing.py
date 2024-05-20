from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd

# Elasticsearch configuration
ES_HOST = "big-data.es.europe-west2.gcp.elastic-cloud.com"
ES_PORT = 443
ES_SCHEME = "https"
INDEX_NAME = "cinema"

# Load the Parquet data into a Pandas DataFrame
file_path = "/Users/abishek/datalake/combined.parquet"
df = pd.read_parquet(file_path)

# Prepare the data for bulk indexing
data = df.to_dict(orient="records")

# Initialize Elasticsearch client with basic authentication
es = Elasticsearch(
    [{"host": ES_HOST, "port": ES_PORT, "scheme": ES_SCHEME}],
    basic_auth=("elastic", "egmDnSWg8mqtiqe1hjQryK6Q")
)

# Define a function to index documents in batches
def index_documents(documents):
    actions = [
        {
            "_index": INDEX_NAME,
            "_source": doc
        }
        for doc in documents
    ]
    bulk(es, actions)

# Index the documents in batches
batch_size = 1000
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    index_documents(batch)

print("Data indexing completed.")
