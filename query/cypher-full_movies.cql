MATCH (m:Movie)-[:BELONGS_TO]->(g:Genre)
RETURN 
    m.id AS movie_id,
    m.title AS movie_title,
    g.id AS genre_id,
    g.name AS genre_name