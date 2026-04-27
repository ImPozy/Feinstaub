SELECT 
    MAX(temperature) AS max_temp,
    MIN(temperature) AS min_temp,
    AVG(temperature) AS avg_temp
FROM weather
WHERE DATE(timestamp) = '2023-03-14';