from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, when, expr, unix_timestamp, from_unixtime, floor, min as spark_min, max as spark_max
import os
import shutil
import glob

# ------------------ CONFIG ------------------
TEAM_NO = "001"
CPU_CSV = "/home/pes2ug23cs313/real_time_server_monitoring/data/processed/cpu_data.csv"
MEM_CSV = "/home/pes2ug23cs313/real_time_server_monitoring/data/processed/mem_data.csv"
OUTPUT_DIR = "/home/pes2ug23cs313/real_time_server_monitoring/data/alerts"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"team_{TEAM_NO}_CPU_MEM.csv")

CPU_THRESHOLD = 81.69
MEM_THRESHOLD = 76.53

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------ INIT SPARK ------------------
spark = SparkSession.builder \
    .appName("CPU_MEM_Alerting_Batch") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ------------------ READ CSV ------------------
cpu_df_raw = spark.read.option("header", True).csv(CPU_CSV)
mem_df_raw = spark.read.option("header", True).csv(MEM_CSV)

# ------------------ PARSE TIMESTAMP ------------------
cpu_df = cpu_df_raw.withColumn("server_id", col("server_id").cast("string")) \
    .withColumn("cpu_pct", col("cpu_pct").cast("float"))

mem_df = mem_df_raw.withColumn("server_id", col("server_id").cast("string")) \
    .withColumn("mem_pct", col("mem_pct").cast("float"))

# ------------------ JOIN CPU + MEMORY ------------------
joined_df = cpu_df.join(mem_df, ["server_id", "ts"], "inner")

# Convert timestamp to seconds since midnight
joined_df = joined_df.withColumn(
    "time_sec",
    unix_timestamp(col("ts"), "HH:mm:ss") % 86400
)

# ------------------ GET DATA TIME BOUNDARIES ------------------
time_bounds = joined_df.agg(
    spark_min("time_sec").alias("min_sec"),
    spark_max("time_sec").alias("max_sec")
).collect()[0]

min_time_sec = time_bounds["min_sec"]
max_time_sec = time_bounds["max_sec"]

# Calculate valid window start range
# First window starts at the first 10-second boundary at or before min_time
# Last window starts such that it still captures data (window can extend beyond max by up to 29 seconds)
first_window_start = (min_time_sec // 10) * 10
last_window_start = (max_time_sec // 10) * 10

print(f"Data time range: {min_time_sec} to {max_time_sec} seconds")
print(f"Valid window start range: {first_window_start} to {last_window_start}")
print(f"Expected number of windows: {int((last_window_start - first_window_start) / 10) + 1}")

# ------------------ COMPUTE WINDOW ASSIGNMENTS ------------------
from pyspark.sql.functions import array, filter as sql_filter, explode

# For each record at time T, it belongs to windows starting at:
# T-20, T-10, T (all rounded down to nearest 10)
# Only include windows where: first_window_start <= window_start <= last_window_start
joined_df = joined_df.withColumn(
    "base_window",
    floor(col("time_sec") / 10) * 10
).withColumn(
    "window_starts",
    sql_filter(
        array(
            col("base_window") - 20,
            col("base_window") - 10,
            col("base_window")
        ),
        lambda x: (
            (x >= first_window_start) &    # Window start >= first valid window
            (x <= last_window_start) &      # Window start <= last valid window  
            (col("time_sec") >= x) &        # Record time >= window start
            (col("time_sec") < x + 30)      # Record time < window end
        )
    )
)

# Explode to create one row per window
joined_exploded = joined_df.withColumn("window_start_sec", explode(col("window_starts")))

# Add window end
joined_exploded = joined_exploded.withColumn("window_end_sec", col("window_start_sec") + 30)

# ------------------ AGGREGATE BY WINDOW ------------------
aggregated_df = joined_exploded.groupBy("server_id", "window_start_sec", "window_end_sec").agg(
    round(avg("cpu_pct"), 2).alias("avg_cpu"),
    round(avg("mem_pct"), 2).alias("avg_mem")
)

# ------------------ ALERT LOGIC ------------------
alerts_df = aggregated_df.withColumn(
    "alert",
    when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD), 
         "High CPU + Memory stress")
    .when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD), 
          "CPU spike suspected")
    .when((col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD), 
          "Memory saturation suspected")
    .otherwise("")
)

# ------------------ FORMAT OUTPUT ------------------
final_df = alerts_df.withColumn(
    "window_start",
    from_unixtime(col("window_start_sec"), "HH:mm:ss")
).withColumn(
    "window_end",
    from_unixtime(col("window_end_sec"), "HH:mm:ss")
).select(
    "server_id",
    "window_start",
    "window_end",
    "avg_cpu",
    "avg_mem",
    "alert"
).orderBy("server_id", "window_start")

# ------------------ VALIDATE ROW COUNT ------------------
row_count = final_df.count()
window_count = final_df.select("window_start").distinct().count()
server_count = final_df.select("server_id").distinct().count()

print(f"\n=== FINAL OUTPUT ===")
print(f"Unique windows: {window_count}")
print(f"Unique servers: {server_count}")
print(f"Total data rows: {row_count}")
print(f"Total rows with header: {row_count + 1}")

# Show sample
print("\nFirst 20 rows:")
final_df.show(20, truncate=False)

print("\nLast 20 rows:")
final_df.orderBy(col("window_start").desc(), "server_id").show(20, truncate=False)

# ------------------ WRITE OUTPUT ------------------
if row_count > 0:
    temp_folder = os.path.join(OUTPUT_DIR, "temp_alerts")
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_folder)
    part_file = glob.glob(os.path.join(temp_folder, "part-*.csv"))[0]
    shutil.move(part_file, OUTPUT_CSV)
    shutil.rmtree(temp_folder)
    print(f"\n✓ Alerts written to {OUTPUT_CSV}")
else:
    print("No alerts generated; skipping CSV write.")

# ------------------ CLEANUP ------------------
spark.stop()
print("\nSpark batch job completed.")
