-- Bronze layer: Raw data ingestion
CREATE OR REFRESH STREAMING TABLE taxi_raw_records 
(CONSTRAINT valid_distance EXPECT (trip_distance > 0.0) ON VIOLATION DROP ROW)
AS SELECT *
FROM STREAM(samples.nyctaxi.trips);

-- Silver layer 1: Flagged rides
CREATE OR REFRESH STREAMING TABLE flagged_rides 
AS SELECT
  date_trunc("week", tpep_pickup_datetime) as week,
  pickup_zip as zip, 
  fare_amount, trip_distance
FROM
  STREAM(LIVE.taxi_raw_records)
WHERE ((pickup_zip = dropoff_zip AND fare_amount > 50) OR
       (trip_distance < 5 AND fare_amount > 50));

-- Silver layer 2: Weekly statistics
CREATE OR REFRESH MATERIALIZED VIEW weekly_stats
AS SELECT
  date_trunc("week", tpep_pickup_datetime) as week,
  AVG(fare_amount) as avg_amount,
  AVG(trip_distance) as avg_distance
FROM
 live.taxi_raw_records
GROUP BY week
ORDER BY week ASC;

-- Gold layer: Top N rides to investigate
CREATE OR REPLACE MATERIALIZED VIEW top_n
AS SELECT
  weekly_stats.week,
  ROUND(avg_amount,2) as avg_amount, 
  ROUND(avg_distance,3) as avg_distance,
  fare_amount, trip_distance, zip 
FROM live.flagged_rides
LEFT JOIN live.weekly_stats ON weekly_stats.week = flagged_rides.week
ORDER BY fare_amount DESC
LIMIT 3;
