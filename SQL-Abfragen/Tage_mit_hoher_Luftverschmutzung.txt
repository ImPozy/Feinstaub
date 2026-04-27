SELECT 
    DATE(timestamp) AS datum,
    MAX(P1) AS max_pm10
FROM air_quality
GROUP BY DATE(timestamp)
HAVING MAX(P1) > 50
ORDER BY max_pm10 DESC;