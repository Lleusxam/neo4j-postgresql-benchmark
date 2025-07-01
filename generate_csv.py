import csv
import os
from faker import Faker
import random

fake = Faker()
NUM_USERS = 2000
NUM_MOVIES = 500
GENRES = ["Action", "Comedy", "Drama", "Science Fiction", "Romance", "Horror", "Documentary", "Animation"]

# Create 'data' directory if it doesn't exist
folder = "data"
os.makedirs(folder, exist_ok=True)

# Generate users
with open(os.path.join(folder, "users.csv"), "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name"])
    for i in range(NUM_USERS):
        writer.writerow([i+1, fake.name()])

# Generate genres
with open(os.path.join(folder, "genres.csv"), "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name"])
    for i, genre in enumerate(GENRES):
        writer.writerow([i+1, genre])

# Generate movies
with open(os.path.join(folder, "movies.csv"), "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "title", "genre_id"])
    for i in range(NUM_MOVIES):
        genre_id = random.randint(1, len(GENRES))
        title = f"Movie {i+1}"
        writer.writerow([i+1, title, genre_id])

# Generate ratings
with open(os.path.join(folder, "ratings.csv"), "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "movie_id", "rating"])
    for user_id in range(1, NUM_USERS + 1):
        num_ratings = random.randint(1, 10)
        rated_movies = random.sample(range(1, NUM_MOVIES + 1), num_ratings)
        for movie_id in rated_movies:
            rating = random.randint(1, 5)
            writer.writerow([user_id, movie_id, rating])
