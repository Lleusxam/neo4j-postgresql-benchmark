import csv
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Loading .env file
load_dotenv()

URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def import_genres(session, file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            session.run(
                "MERGE (g:Genre {id: $id}) SET g.name = $name",
                id=int(row['id']),
                name=row['name']
            )

def import_movies(session, file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            session.run(
                """
                MERGE (m:Movie {id: $id})
                SET m.title = $title
                WITH m
                MATCH (g:Genre {id: $genre_id})
                MERGE (m)-[:BELONGS_TO]->(g)
                """,
                id=int(row['id']),
                title=row['title'],
                genre_id=int(row['genre_id'])
            )

def import_users(session, file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            session.run(
                "MERGE (u:User {id: $id}) SET u.name = $name",
                id=int(row['id']),
                name=row['name']
            )

def import_ratings(session, file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            session.run(
                """
                MATCH (u:User {id: $user_id})
                MATCH (m:Movie {id: $movie_id})
                MERGE (u)-[r:RATED]->(m)
                SET r.rating = $rating
                """,
                user_id=int(row['user_id']),
                movie_id=int(row['movie_id']),
                rating=int(row['rating'])
            )

def main():
    with driver.session() as session:
        print("Importing genres...")
        import_genres(session, 'data/genres.csv')
        print("Importing movies...")
        import_movies(session, 'data/movies.csv')
        print("Importing users...")
        import_users(session, 'data/users.csv')
        print("Importing ratings...")
        import_ratings(session, 'data/ratings.csv')
    driver.close()
    print("Import complete.")

if __name__ == "__main__":
    main()
