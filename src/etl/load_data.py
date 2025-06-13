import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv("../../config/.env")


def connect():
    db = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    return db


def create_table(db):
    cursor = db.cursor()
    create = """ 
    CREATE TABLE IF NOT EXISTS movie (
      imdb_id VARCHAR(10) PRIMARY KEY,
      title VARCHAR(255),
      year INTEGER,
      rated VARCHAR(10),
      released DATE,
      runtime INTEGER,
      genre VARCHAR(255),
      director VARCHAR(255),
      writer VARCHAR(255),
      actors VARCHAR(255),
      plot TEXT,
      language VARCHAR(100),
      country VARCHAR(100),
      awards VARCHAR(255),
      poster VARCHAR(255),
      metascore INTEGER,
      imdb_rating FLOAT,
      imdb_votes INTEGER,
      box_office VARCHAR(50)
    )
    """
    cursor.execute(create)
    db.commit()


def insert_movie(db, movie_data):
    cursor = db.cursor()
    insert = (
        "REPLACE INTO movie (imdb_id, title, year, rated, released, runtime, genre, "
        "director, writer, actors, plot, language, country, awards, poster, metascore, imdb_rating, imdb_votes, box_office) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    data = (
        movie_data["imdbID"],
        movie_data["Title"],
        (
            int(movie_data["Year"].split("–")[0])
            if "-" in movie_data["Year"]
            else int(movie_data["Year"])
        ),
        movie_data["Rated"],
        movie_data["Released"],
        int(movie_data["Runtime"].split()[0]),
        movie_data["Genre"],
        movie_data["Director"],
        movie_data["Writer"],
        movie_data["Actors"],
        movie_data["Plot"],
        movie_data["Language"],
        movie_data["Country"],
        movie_data["Awards"],
        movie_data["Poster"],
        int(movie_data["Metascore"]) if movie_data["Metascore"].isdigit() else 0,
        float(movie_data["imdbRating"]) if movie_data["imdbRating"] != "N/A" else 0.0,
        (
            int(movie_data["imdbVotes"].replace(",", ""))
            if movie_data["imdbVotes"] != "N/A"
            else 0
        ),
        movie_data["BoxOffice"],
    )
    cursor.execute(insert)
    db.commit()


def main():
    db = connect()
    create_table(db)
    # Ideally you'd fetch first, then insert
    from fetch_data import get_movie

    titles = ["Inception", "Matrix", "Jurassic Park"]

    for title in titles:
        movie = get_movie(title)
        if movie["Response"] == "True":
            insert_movie(db, movie)


if __name__ == "__main__":
    main()
