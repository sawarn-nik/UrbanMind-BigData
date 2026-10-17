/*
 * UrbanMind: Pig Latin ETL. Turns MapReduce zone-hour aggregates into a model-ready feature table.
 *
 *   1. drop zone-hours with too few trips to give a stable speed
 *   2. free-flow speed per zone   = average speed in the quiet night hours (1-5 AM)
 *   3. join zone names/boroughs   (replicated join: small lookup held in memory)
 *   4. join hourly weather        (replicated LEFT OUTER join)
 *   5. congestion index           = 1 - speed / free-flow speed, clipped to [0, 1]
 *      level                      = LOW / MODERATE / HEAVY
 *
 * Parameters: MR, ZONES, WEATHER, OUT, TOP
 */

zh_all  = LOAD '$MR' USING PigStorage('\t')
          AS (zone_id:int, dt:chararray, hr:int, trips:long, miles:double, avg_speed:double, avg_dur:double);
zones   = LOAD '$ZONES' USING PigStorage('\t')
          AS (zone_id:int, borough:chararray, zone:chararray, service_zone:chararray);
weather = LOAD '$WEATHER' USING PigStorage('\t')
          AS (dt:chararray, hr:int, temp_c:double, precip_mm:double, snow_cm:double, wind_kmh:double);

zh = FILTER zh_all BY trips >= 5;

-- free-flow speed per zone
night      = FILTER zh BY hr >= 1 AND hr <= 5;
night_g    = GROUP night BY zone_id;
night_ff   = FOREACH night_g GENERATE group AS zone_id, AVG(night.avg_speed) AS ff_night;
all_g      = GROUP zh BY zone_id;
zone_stats = FOREACH all_g GENERATE group AS zone_id, AVG(zh.avg_speed) AS mean_speed, SUM(zh.trips) AS total_trips;
ff_j       = JOIN zone_stats BY zone_id LEFT OUTER, night_ff BY zone_id;
freeflow   = FOREACH ff_j GENERATE
               zone_stats::zone_id AS zone_id,
               zone_stats::total_trips AS total_trips,
               (night_ff::ff_night IS NULL OR night_ff::ff_night < zone_stats::mean_speed
                  ? zone_stats::mean_speed * 1.25 : night_ff::ff_night) AS freeflow;

-- attach free-flow speed, zone names, weather
a  = JOIN zh BY zone_id, freeflow BY zone_id USING 'replicated';
a1 = FOREACH a GENERATE zh::zone_id AS zone_id, zh::dt AS dt, zh::hr AS hr, zh::trips AS trips,
                        zh::avg_speed AS avg_speed, zh::avg_dur AS avg_dur, freeflow::freeflow AS freeflow;
b  = JOIN a1 BY zone_id, zones BY zone_id USING 'replicated';
b1 = FOREACH b GENERATE a1::zone_id AS zone_id, zones::borough AS borough, zones::zone AS zone,
                        a1::dt AS dt, a1::hr AS hr, a1::trips AS trips, a1::avg_speed AS avg_speed,
                        a1::avg_dur AS avg_dur, a1::freeflow AS freeflow;
c  = JOIN b1 BY (dt, hr) LEFT OUTER, weather BY (dt, hr) USING 'replicated';

c1 = FOREACH c GENERATE
       b1::zone_id AS zone_id, b1::borough AS borough, b1::zone AS zone, b1::dt AS dt, b1::hr AS hr,
       (int) ToString(ToDate(b1::dt, 'yyyy-MM-dd'), 'e') AS dow,
       b1::trips AS trips, b1::avg_speed AS avg_speed, b1::avg_dur AS avg_dur,
       (weather::temp_c    IS NULL ? 0.0 : weather::temp_c)    AS temp_c,
       (weather::precip_mm IS NULL ? 0.0 : weather::precip_mm) AS precip_mm,
       (weather::snow_cm   IS NULL ? 0.0 : weather::snow_cm)   AS snow_cm,
       (weather::wind_kmh  IS NULL ? 0.0 : weather::wind_kmh)  AS wind_kmh,
       b1::freeflow AS freeflow,
       1.0 - b1::avg_speed / b1::freeflow AS ci_raw;

features = FOREACH c1 GENERATE
       zone_id, borough, zone, dt, hr, dow, (dow >= 6 ? 1 : 0) AS is_weekend,
       trips, avg_speed, avg_dur, temp_c, precip_mm, snow_cm, wind_kmh,
       ROUND_TO(freeflow, 3) AS freeflow,
       ROUND_TO((ci_raw < 0.0 ? 0.0 : (ci_raw > 1.0 ? 1.0 : ci_raw)), 4) AS congestion_index,
       (ci_raw < 0.25 ? 'LOW' : (ci_raw < 0.45 ? 'MODERATE' : 'HEAVY')) AS level;

STORE features INTO '$OUT' USING PigStorage('\t');

-- side output: busiest 15 zones by trip volume
zs_named = JOIN freeflow BY zone_id, zones BY zone_id USING 'replicated';
zs_flat  = FOREACH zs_named GENERATE zones::zone_id AS zone_id, zones::borough AS borough, zones::zone AS zone,
                                     freeflow::total_trips AS total_trips, ROUND_TO(freeflow::freeflow, 2) AS freeflow_mph;
ranked   = ORDER zs_flat BY total_trips DESC;
top      = LIMIT ranked 15;
STORE top INTO '$TOP' USING PigStorage('\t');
