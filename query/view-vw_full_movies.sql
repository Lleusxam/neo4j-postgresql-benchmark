CREATE OR REPLACE VIEW vw_full_movies AS
SELECT
    m.id AS movie_id,
    m.title AS movie_title,
    g.id AS genre_id,
    g.name AS genre_name
FROM
    movies m
JOIN genres g ON g.id = m.genre_id;