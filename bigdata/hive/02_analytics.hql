-- UrbanMind: congestion analytics in HiveQL
USE urbanmind;
SET hive.exec.mode.local.auto=true;

-- Q1. Scale: how many raw trips are in the warehouse (full scan of the raw CSVs)
SELECT 'Q1 raw trips' AS q, COUNT(*) AS trips, ROUND(SUM(distance_mi)) AS total_miles FROM raw_trips;

-- Q2. City-wide hourly profile, weekday vs weekend
SELECT 'Q2' AS q, hr,
       ROUND(AVG(CASE WHEN is_weekend = 0 THEN avg_speed END), 2)        AS weekday_mph,
       ROUND(AVG(CASE WHEN is_weekend = 1 THEN avg_speed END), 2)        AS weekend_mph,
       ROUND(AVG(CASE WHEN is_weekend = 0 THEN congestion_index END), 3) AS weekday_ci
FROM congestion GROUP BY hr ORDER BY hr;

-- Q3. Ten most congested zones in the weekday evening peak (5-7 PM)
SELECT 'Q3' AS q, zone, borough, ROUND(AVG(avg_speed), 2) AS peak_mph, ROUND(AVG(avg_ci), 3) AS peak_ci, SUM(trips) AS trips
FROM weekday_peak WHERE hr IN (17, 18, 19)
GROUP BY zone, borough HAVING SUM(trips) > 5000
ORDER BY peak_ci DESC LIMIT 10;

-- Q4. Borough comparison
SELECT 'Q4' AS q, borough, COUNT(*) AS zone_hours, SUM(trips) AS trips,
       ROUND(AVG(avg_speed), 2) AS avg_mph, ROUND(AVG(congestion_index), 3) AS avg_ci,
       ROUND(100 * SUM(CASE WHEN level = 'HEAVY' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_heavy
FROM congestion GROUP BY borough ORDER BY trips DESC;

-- Q5. Weather impact: rainy vs dry hours in Manhattan
SELECT 'Q5' AS q, CASE WHEN precip_mm >= 0.5 THEN 'rain' ELSE 'dry' END AS weather,
       COUNT(*) AS zone_hours, ROUND(AVG(avg_speed), 2) AS avg_mph, ROUND(AVG(congestion_index), 3) AS avg_ci
FROM congestion WHERE borough = 'Manhattan' AND hr BETWEEN 7 AND 20
GROUP BY CASE WHEN precip_mm >= 0.5 THEN 'rain' ELSE 'dry' END;

-- Q6. Partition pruning: only the 2024-03 partition is read
SELECT 'Q6 March' AS q, level, COUNT(*) AS zone_hours FROM congestion WHERE month = '2024-03' GROUP BY level;
EXPLAIN DEPENDENCY SELECT AVG(avg_speed) FROM congestion WHERE month = '2024-03';

-- Q7. Bucket sampling: read 1 of 8 buckets (~1/8 of the zones) for a fast estimate
SELECT 'Q7 sample' AS q, COUNT(DISTINCT zone_id) AS zones_in_sample, ROUND(AVG(congestion_index), 3) AS est_ci
FROM congestion TABLESAMPLE (BUCKET 1 OUT OF 8 ON zone_id);

-- Q8. Monthly trend
SELECT 'Q8' AS q, month, SUM(trips) AS trips, ROUND(AVG(avg_speed), 2) AS avg_mph, ROUND(AVG(congestion_index), 3) AS avg_ci
FROM congestion GROUP BY month ORDER BY month;
