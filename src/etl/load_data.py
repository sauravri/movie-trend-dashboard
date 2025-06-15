import mysql.connector
from dotenv import load_dotenv
import os
import requests
import logging

from utils import (
    fallback_int,
    fallback_float,
    fallback_year,
    fallback_str,
    fallback_genre_ids,
)

# =================================================================================================
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


# =================================================================================================
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
            popularity FLOAT
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


def movie_exists(cursor, title, release_year):
    """Check if a movie already exists in the db."""
    cursor.execute(
        "SELECT id FROM movies WHERE title = %s AND release_year = %s LIMIT 1",
        (title, release_year),
    )
    return cursor.fetchone()


def insert_movie(
    conn, cursor, title, release_year, vote_average, popularity, genre_ids
):
    """Insert movie with genres into the database."""
    try:
        # Insert movie first
        cursor.execute(
            """
            INSERT INTO movies (title, release_year, vote_average, popularity)
            VALUES (%s, %s, %s, %s)
            """,
            (title, release_year, vote_average, popularity),
        )
        movie_id = cursor.lastrowid

        # Insert genres and link them to the movie
        response = requests.get(
            f"https://api.themoviedb.org/3/genre/movie/list?api_key={TMDB_API_KEY}",
            timeout=10,
        )
        response.raise_for_status()
        genres = response.json().get("genres", [])
        genres_map = {g["id"]: g["name"] for g in genres}

        for genre_id in genre_ids:
            genre_name = genres_map.get(genre_id)
            if genre_name:
                try:
                    cursor.execute(
                        "INSERT IGNORE INTO genres (name) VALUES (%s)", (genre_name,)
                    )
                except mysql.connector.Error as e:
                    logging.error(f"Error inserting genre {genre_name}. Exception: {e}")

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
        logging.info(f"Movie {title} successfully added.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting movie {title}. Exception: {e}")


def fetch_movie(id=None, page=None):
    """Fetch popular movies or by id from TMDB API with error handling."""
    try:
        if id:
            response = requests.get(
                f"https://api.themoviedb.org/3/movie/{id}?api_key={TMDB_API_KEY}",
                timeout=10,
            )
        elif page:
            response = requests.get(
                f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&page={page}",
                timeout=10,
            )
        if response.status_code == 400:
            logging.error("Received 400. Breaking pagination.")
            return None

        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logging.error(f"Error retrieving from TMDB API: {e}")
        return {}  # fallback to empty


# =================================================================================================
def get_safe_field(data, field_name, fallback=None, transform=None):
    """Safely extract a field from a dictionary with fallback and transformation if needed."""
    value = data.get(field_name)
    if not value or value == "" or value is None:
        return fallback
    if transform:
        try:
            return transform(value)
        except (ValueError, TypeError):
            return fallback
    return value


# ============================================================================================
def main():
    """Main pipeline to fetch popular movies and insert into db with genres."""
    create_table()

    conn = connect()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        page = 1
        total_pages = 1

        while page <= total_pages:
            logging.info(f"Fetching page {page}.")
            data = fetch_movie(page=page)

            if not data:
                logging.error("No data or pagination halted.")
                break

            total_pages = data.get("total_pages", 1)

            for movie in data.get("results", []):

                title = movie.get("title") or "Unknown Title"

                release_year = get_safe_field(
                    movie,
                    "release_date",
                    fallback=None,
                    transform=lambda x: int(x.split("-")[0]),
                )

                vote_average = get_safe_field(
                    movie, "vote_average", fallback=0.0, transform=float
                )

                popularity = get_safe_field(
                    movie, "popularity", fallback=0.0, transform=float
                )

                genre_ids = movie.get("genre_ids") or []

                if movie_exists(cursor, title, release_year):
                    logging.info(f"Movie {title} already exists. Skipping.")
                    continue

                insert_movie(
                    conn,
                    cursor,
                    title,
                    release_year,
                    vote_average,
                    popularity,
                    genre_ids,
                )

            page += 1

        logging.info("Data is ready.")
        print("Data is ready!")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during main. Exception: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
