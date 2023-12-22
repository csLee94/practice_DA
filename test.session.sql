SELECT
    t1.date,
    COUNT(DISTINCT t2.user_id) as running_users
FROM(
    SELECT
        DATE(date) as date,
        user_id
    FROM 
        logs
    WHERE date >='2023-11-01' AND action_type = "booking"
) AS t1
LEFT JOIN
    (SELECT DATE(date) as date, user_id  FROM logs WHERE date >='2023-11-01' AND action_type = "booking") as t2
ON 
    t2.date  BETWEEN DATE_SUB(t1.date, INTERVAL 6 DAY) AND t1.date 
GROUP BY 
    t1.date
;