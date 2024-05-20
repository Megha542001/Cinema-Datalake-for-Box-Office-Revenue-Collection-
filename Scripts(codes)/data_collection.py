from datetime import datetime, timedelta
import requests
import json
import logging

def collect_tmdb_data():
    try:
        logging.info("Starting TMDB data collection")
        # Fetch movie data from TMDB API for 2023 and 2022
        tmdb_api_key = 'bfd1fa0b47958850ad4d9f1606f1c15a'
        current_year = datetime.now().year
        years = [current_year - 1, current_year]  # Collect movies from previous year and current year

        movie_data = []
        for year in years:
            tmdb_url = f'https://api.themoviedb.org/3/discover/movie?api_key={tmdb_api_key}&primary_release_year={year}'
            tmdb_response = requests.get(tmdb_url)
            tmdb_response.raise_for_status()  # Raises an exception if the request fails
            tmdb_year_data = tmdb_response.json()

            # Iterate over movies and fetch IMDb ID for each movie
            for movie in tmdb_year_data['results']:
                movie_id = movie['id']
                imdb_url = f'https://api.themoviedb.org/3/movie/{movie_id}/external_ids?api_key={tmdb_api_key}'
                imdb_response = requests.get(imdb_url)
                imdb_response.raise_for_status()  # Raises an exception if the request fails
                imdb_data = imdb_response.json()
                imdb_id = imdb_data.get('imdb_id', None)

                movie['imdb_id'] = imdb_id
                movie_data.append(movie)

        # Save TMDB data in a local file
        tmdb_file_path = '/Users/abishek/datalake/tmdb_movie.json'
        with open(tmdb_file_path, 'w') as file:
            json.dump(movie_data, file)

        logging.info("Finished TMDB data collection")

    except Exception as e:
        logging.error(f"Error occurred: {e}")


def collect_omdb_data():
    try:
        logging.info("Starting OMDB data collection")
        # Fetch movie data from OMDB API for 2023 and 2022
        omdb_api_key = '2f33835b'
        current_year = datetime.now().year
        years = [current_year - 1, current_year]  # Collect movies from previous year and current year

        movie_data = []
        for year in years:
            omdb_url = f'http://www.omdbapi.com/?apikey={omdb_api_key}&s=movie&y={year}&page=1'
            omdb_response = requests.get(omdb_url)
            omdb_response.raise_for_status()  # Raises an exception if the request fails
            omdb_year_data = omdb_response.json()
            total_pages = (int(omdb_year_data['totalResults']) // 10) + 1  # Each page contains 10 results

            # Collect data for all pages
            for page in range(1, total_pages + 1):
                omdb_url = f'http://www.omdbapi.com/?apikey={omdb_api_key}&s=movie&y={year}&page={page}'
                omdb_response = requests.get(omdb_url)
                omdb_response.raise_for_status()  # Raises an exception if the request fails
                omdb_page_data = omdb_response.json()
                movie_data.extend(omdb_page_data['Search'])

        # Save OMDB data in a local file
        omdb_file_path = '/Users/abishek/datalake/omdb_movie.json'
        with open(omdb_file_path, 'w') as file:
            json.dump(movie_data, file)

        logging.info("Finished OMDB data collection")

    except Exception as e:
        logging.error(f"Error occurred: {e}")


# Run the tasks
collect_tmdb_data()
collect_omdb_data()
