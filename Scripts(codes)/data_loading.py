import requests
import json

ES_ENDPOINT = "https://big-data.es.europe-west2.gcp.elastic-cloud.com"
INDEX_NAME = "cinema"
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "adult": {"type": "boolean"},
            "backdrop_path": {"type": "text"},
            "genre_ids": {"type": "keyword"},
            "imdb_id": {"type": "keyword"},
            "original_language": {"type": "keyword"},
            "original_title": {"type": "text"},
            "overview": {"type": "text"},
            "popularity": {"type": "float"},
            "poster_path": {"type": "text"},
            "release_date": {"type": "date", "format": "epoch_second"},
            "title": {"type":"keyword"},
            "video": {"type": "keyword"},
            "vote_average": {"type": "float"},
            "vote_count": {"type": "integer"},
            "release_date_utc": {"type": "date"},
            "Poster": {"type": "text"},
            "Type": {"type": "keyword"},
            "Year": {"type": "integer"},
            "imdbID": {"type": "keyword"}
        }
    }
}

response = requests.put(f"{ES_ENDPOINT}/{INDEX_NAME}",
                        headers={"Content-Type": "application/json"},
                        data=json.dumps(INDEX_MAPPING),
                        auth=("elastic", "egmDnSWg8mqtiqe1hjQryK6Q"))
print(response.json())
