import requests
import os
from dotenv import load_dotenv

load_dotenv('../../config/.env')

API_KEY = os.getenv("OMDB_API_KEY")

def get_movie(title):
    base_url = "http://www.omdbapi.com/"
    response = requests.get(base_url, params={"t": title, "apikey": API_KEY})
    return response.json()


if __name__ == "__main__":
    titles = ['Inception', 'Matrix', 'Jurassic Park']

    for title in titles:
        movie = get_movie(title)
        print(movie)