import csv
import os
import psycopg2
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Connect to PostgreSQL Neon
conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DATABASE"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    port=os.getenv("PG_PORT")
)
cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE IF NOT EXISTS genres (
    id INT PRIMARY KEY,
    name TEXT
);
CREATE TABLE IF NOT EXISTS movies (
    id INT PRIMARY KEY,
    title TEXT,
    genre_id INT REFERENCES genres(id)
);
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name TEXT
);
CREATE TABLE IF NOT EXISTS ratings (
    user_id INT REFERENCES users(id),
    movie_id INT REFERENCES movies(id),
    rating INT,
    PRIMARY KEY (user_id, movie_id)
);
""")
conn.commit()

# Import genres
with open("data/genres.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute(
            "INSERT INTO genres (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (int(row['id']), row['name'])
        )

# Import movies
with open("data/movies.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute(
            "INSERT INTO movies (id, title, genre_id) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (int(row['id']), row['title'], int(row['genre_id']))
        )

# Import users
with open("data/users.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute(
            "INSERT INTO users (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (int(row['id']), row['name'])
        )

# Import ratings
with open("data/ratings.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute(
            "INSERT INTO ratings (user_id, movie_id, rating) VALUES (%s, %s, %s) ON CONFLICT (user_id, movie_id) DO NOTHING",
            (int(row['user_id']), int(row['movie_id']), int(row['rating']))
        )

conn.commit()
cur.close()
conn.close()

print("Data successfully imported into PostgreSQL Neon.")
