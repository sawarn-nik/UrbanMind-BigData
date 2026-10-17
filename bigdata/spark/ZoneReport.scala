// UrbanMind: Spark job in Scala. Reads the model's June predictions from HDFS and reports
// (1) model error per borough vs the historical-average baseline and
// (2) the zones predicted to be most congested in the weekday evening peak.
// Run with:  spark-shell --driver-memory 1g -i ZoneReport.scala
import org.apache.spark.sql.functions._

val pred = spark.read.parquet("hdfs://hadoop:9000/urbanmind/predictions/june_2024")
pred.createOrReplaceTempView("pred")

println("\n== Model error by borough, June 2024 hold-out (mph)")
spark.sql("""
  SELECT borough, COUNT(*) AS zone_hours,
         ROUND(SQRT(AVG(POW(actual_speed - pred_gbt, 2))), 3)       AS rmse_gbt,
         ROUND(SQRT(AVG(POW(actual_speed - baseline_speed, 2))), 3) AS rmse_baseline
  FROM pred GROUP BY borough ORDER BY zone_hours DESC""").show(false)

val peak = pred
  .filter($"hr".between(17, 19) && $"dow" <= 5)
  .groupBy($"zone", $"borough")
  .agg(avg($"pred_ci").as("predCi"), avg($"actual_ci").as("actualCi"), sum($"trips").as("trips"))
  .filter($"trips" > 2000)
  .select($"zone", $"borough", $"predCi", $"actualCi")
  .orderBy(desc("predCi"))
  .limit(10)

println("== Most congested zones, weekday 5-7 PM (predicted vs actual congestion index)")
val rows = peak.collect().map(r => (r.getString(0), r.getString(1), r.getDouble(2), r.getDouble(3)))
rows.zipWithIndex.foreach { case ((zone, borough, predCi, actualCi), i) =>
  println(f"${i + 1}%2d. $zone%-40s $borough%-10s predicted $predCi%.3f   actual $actualCi%.3f")
}
peak.coalesce(1).write.mode("overwrite").option("header", "true").csv("file:///workspace/output/scala_peak_zones")
System.exit(0)
