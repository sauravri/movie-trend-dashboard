import mysql.connector
from dotenv import load_dotenv
import os
import requests

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT") or 3306)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

print("DB_HOST =", DB_HOST)
print("DB_PORT =", DB_PORT)
print("DB_NAME =", DB_NAME)
print("DB_USER =", DB_USER)


def connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def create_table():
    conn = connect()
    cursor = conn.cursor()

    # Create genres table first
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
    """
    )

    # Create movies table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            release_year INTEGER,
            vote_average FLOAT,
            popularity FLOAT,
            box_office INTEGER
        )
    """
    )

    # Create movie_genres table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS movie_genres (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            movie_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movies(id),
            FOREIGN KEY (genre_id) REFERENCES genres(id)
        )
    """
    )

    conn.commit()
    cursor.close()
    conn.close()


def insert_movie(title, release_year, vote_average, popularity, box_office, genre_ids):
    conn = connect()
    cursor = conn.cursor()

    # Insert movie first
    cursor.execute(
        """
        INSERT INTO movies (title, release_year, vote_average, popularity, box_office)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (title, release_year, vote_average, popularity, box_office),
    )
    conn.commit()
    movie_id = cursor.lastrowid

    # Insert genres
    for genre_id in genre_ids:
        # Check if genre already exists
        response = requests.get(
            f"https://api.themoviedb.org/3/genre/movie/list?api_key={TMDB_API_KEY}"
        )
        genres = response.json().get("genres", [])
        for g in genres:
            if g["id"] == genre_id:
                genre_name = g["name"]

                # Insert or reuse genre
                try:
                    cursor.execute(
                        "INSERT IGNORE INTO genres (name) VALUES (%s)", (genre_name,)
                    )
                    conn.commit()
                except mysql.connector.Error as e:
                    print("Error inserting genre.", e)

                # Retrieve its id
                cursor.execute(
                    "SELECT id FROM genres WHERE name = %s LIMIT 1", (genre_name,)
                )
                row = cursor.fetchone()
                genre_db_id = row[0]

                # Link to movie
                cursor.execute(
                    "INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s)",
                    (movie_id, genre_db_id),
                )
                conn.commit()

                break

    cursor.close()
    conn.close()


def fetch_movie(id=None, page=None):
    """Fetch popular movies from TMDB API."""
    if id:
        response = requests.get(
            f"https://api.themoviedb.org/3/movie/{id}?api_key={TMDB_API_KEY}"
        )
        return response.json()
    elif page:
        response = requests.get(
            f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&page={page}"
        )
        return response.json()


def main():
    create_table()
    for page in range(1, 5):  # 4 pages = 80 popular movies
        data = fetch_movie(page=page)
        for movie in data["results"]:
            insert_movie(
                movie["title"],
                int(movie["release_date"].split("-")[0]),
                movie["vote_average"],
                movie["popularity"],
                0,  # box_office is not available here
                movie["genre_ids"],
            )
    
    print("Data is ready!")


if __name__ == "__main__":
    main()
