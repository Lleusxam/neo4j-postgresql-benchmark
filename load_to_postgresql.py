import csv
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Connect to PostgreSQL Neon
try:
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT"),
        sslmode=os.getenv("PG_SSLMODE") 
    )
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

cur = conn.cursor()

# Create tables
try:
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
    print("Tables created successfully.")
except Exception as e:
    print(f"Error creating tables: {e}")
    conn.rollback()
    conn.close()
    exit(1)

# Helper function to import CSV
def import_csv(file_path, query, transform_func=None):
    try:
        with open(file_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                values = transform_func(row) if transform_func else row
                cur.execute(query, values)
                count += 1
            print(f"{count} records imported from {file_path}")
    except Exception as e:
        print(f"Error importing {file_path}: {e}")
        conn.rollback()
        conn.close()
        exit(1)

# Import genres
import_csv(
    "data/genres.csv",
    "INSERT INTO genres (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
    lambda row: (int(row['id']), row['name'])
)

# Import movies
import_csv(
    "data/movies.csv",
    "INSERT INTO movies (id, title, genre_id) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
    lambda row: (int(row['id']), row['title'], int(row['genre_id']))
)

# Import users
import_csv(
    "data/users.csv",
    "INSERT INTO users (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
    lambda row: (int(row['id']), row['name'])
)

# Import ratings
import_csv(
    "data/ratings.csv",
    "INSERT INTO ratings (user_id, movie_id, rating) VALUES (%s, %s, %s) ON CONFLICT (user_id, movie_id) DO NOTHING",
    lambda row: (int(row['user_id']), int(row['movie_id']), int(row['rating']))
)

conn.commit()
cur.close()
conn.close()

print("Data successfully imported into PostgreSQL Neon.")
