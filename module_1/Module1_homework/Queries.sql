-- ANSWER TO QUESTION 3
SELECT
       COUNT(*) AS TotalTrips
FROM
    green_taxi_data G
WHERE
    CAST(G.lpep_dropoff_datetime AS DATE) = '2019-09-18' AND
    CAST(G.lpep_pickup_datetime AS DATE) = '2019-09-18';

-- ANSWER TO QUESTION 4
SELECT
    lpep_pickup_datetime
FROM
    green_taxi_data G
ORDER BY
    trip_distance DESC
LIMIT 1;

SELECT
    CAST(lpep_pickup_datetime AS DATE) AS Pickup_Day,
    MAX(trip_distance) AS MaxTripDistance
FROM
    public.green_taxi_data
GROUP BY
    Pickup_Day
ORDER BY
    MaxTripDistance DESC
LIMIT 1;


-- ANSWER TO QUESTION 5
SELECT
    z."Borough",
    SUM(g.total_amount) AS total_amount
FROM
    green_taxi_data AS g
JOIN
        taxi_zone_data AS z
ON
    g."PULocationID" = z."LocationID"
WHERE
    CAST(g.lpep_pickup_datetime AS DATE) = '2019-09-18' AND
    z."Borough" != 'Unknown'
GROUP BY
    z."Borough"
HAVING
    SUM(g.total_amount)  > 50000
ORDER BY
    total_amount DESC;


-- ANSWER TO QUESTION 6
SELECT
    z."Zone",
    g."DOLocationID",
    max(g.tip_amount)
FROM green_taxi_data g
    JOIN taxi_zone_data z
        ON G."PULocationID" = z."LocationID"
WHERE
    to_char(g.lpep_pickup_datetime, 'YYYY-MM') = '2019-09' AND
    z."Zone" = 'Astoria'
GROUP BY Z."Zone", g."DOLocationID"
ORDER BY max(g.tip_amount) DESC


