CREATE OR REPLACE VIEW vw_full_ratings AS
SELECT
    u.id AS user_id,
    u.name AS user_name,
    m.*
FROM
    ratings r
JOIN users u ON u.id = r.user_id
JOIN vw_full_movies m ON m.movie_id = r.movie_id;
