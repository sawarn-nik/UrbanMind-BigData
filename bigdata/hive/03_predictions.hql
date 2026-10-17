-- UrbanMind: expose the Spark MLlib predictions (Parquet on HDFS) to Hive
USE urbanmind;
SET hive.exec.mode.local.auto=true;

DROP TABLE IF EXISTS predictions;
CREATE EXTERNAL TABLE predictions (
  zone_id INT, borough STRING, zone STRING, dt STRING, hr INT, dow INT, trips BIGINT,
  actual_speed DOUBLE, pred_gbt DOUBLE, pred_lr DOUBLE, baseline_speed DOUBLE, freeflow DOUBLE,
  actual_ci DOUBLE, pred_ci DOUBLE, actual_level STRING, pred_level STRING)
STORED AS PARQUET
LOCATION '/urbanmind/predictions/june_2024';

-- confusion matrix of predicted vs actual congestion level
SELECT 'P1 confusion' AS q, actual_level, pred_level, COUNT(*) AS n
FROM predictions GROUP BY actual_level, pred_level ORDER BY actual_level, pred_level;

-- error by hour of day: where is the model weakest?
SELECT 'P2' AS q, hr, ROUND(AVG(ABS(actual_speed - pred_gbt)), 3) AS mae_gbt,
       ROUND(AVG(ABS(actual_speed - baseline_speed)), 3) AS mae_baseline
FROM predictions GROUP BY hr ORDER BY hr;
