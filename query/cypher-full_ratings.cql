MATCH (u:User)-[r:RATED]->(m:Movie)-[:BELONGS_TO]->(g:Genre)
RETURN
    u.id AS user_id,
    u.name AS user_name,
    m.id AS movie_id,
    m.title AS movie_title,
    g.id AS genre_id,
    g.name AS genre_name,
    r.rating AS rating