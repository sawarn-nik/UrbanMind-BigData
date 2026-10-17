-- UrbanMind: Hive data warehouse over HDFS
CREATE DATABASE IF NOT EXISTS urbanmind COMMENT 'Urban traffic congestion warehouse';
USE urbanmind;

-- 1. Raw trips: EXTERNAL table directly over the CSV files loaded into HDFS (schema-on-read)
DROP TABLE IF EXISTS raw_trips;
CREATE EXTERNAL TABLE raw_trips (
  pickup_ts   STRING,
  dropoff_ts  STRING,
  passengers  DOUBLE,
  distance_mi DOUBLE,
  pu_zone     INT,
  do_zone     INT,
  fare        DOUBLE,
  total       DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/urbanmind/raw/trips'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 2. Zone lookup
DROP TABLE IF EXISTS zones;
CREATE EXTERNAL TABLE zones (zone_id INT, borough STRING, zone STRING, service_zone STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/urbanmind/ref/zones';

-- 3. MapReduce output
DROP TABLE IF EXISTS zone_hour_mr;
CREATE EXTERNAL TABLE zone_hour_mr (
  zone_id INT, dt STRING, hr INT, trips BIGINT, miles DOUBLE, avg_speed DOUBLE, avg_dur DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/urbanmind/mr/zone_hour';

-- 4. Pig output (feature table)
DROP TABLE IF EXISTS zone_hour_features;
CREATE EXTERNAL TABLE zone_hour_features (
  zone_id INT, borough STRING, zone STRING, dt STRING, hr INT, dow INT, is_weekend INT,
  trips BIGINT, avg_speed DOUBLE, avg_dur DOUBLE,
  temp_c DOUBLE, precip_mm DOUBLE, snow_cm DOUBLE, wind_kmh DOUBLE,
  freeflow DOUBLE, congestion_index DOUBLE, level STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/urbanmind/features/zone_hour';

-- 5. Managed ORC table, PARTITIONED by month (partition pruning) and BUCKETED by zone (sampling, joins)
DROP TABLE IF EXISTS congestion;
CREATE TABLE congestion (
  zone_id INT, borough STRING, zone STRING, dt STRING, hr INT, dow INT, is_weekend INT,
  trips BIGINT, avg_speed DOUBLE, temp_c DOUBLE, precip_mm DOUBLE,
  freeflow DOUBLE, congestion_index DOUBLE, level STRING)
PARTITIONED BY (month STRING)
CLUSTERED BY (zone_id) INTO 8 BUCKETS
STORED AS ORC;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE congestion PARTITION (month)
SELECT zone_id, borough, zone, dt, hr, dow, is_weekend, trips, avg_speed, temp_c, precip_mm,
       freeflow, congestion_index, level, substr(dt, 1, 7) AS month
FROM zone_hour_features;

-- 6. Views used by the analytics and the dashboard
CREATE OR REPLACE VIEW weekday_peak AS
SELECT zone_id, borough, zone, hr, AVG(avg_speed) AS avg_speed, AVG(congestion_index) AS avg_ci, SUM(trips) AS trips
FROM congestion
WHERE is_weekend = 0 AND hr IN (8, 9, 17, 18, 19)
GROUP BY zone_id, borough, zone, hr;

SHOW TABLES;
SHOW PARTITIONS congestion;
