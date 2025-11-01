#!/usr/bin/env python3
"""
Minimal Spark Job with Alerts (NET + DISK)
- Input:  net_data.csv (server_id,ts,net_in), disk_data.csv (server_id,ts,disk_io)
- Output: server_id, window_start, window_end, max_net_in, max_disk_io, alert
- Window: 30s size, 10s slide; sorted by server_id then window_start
"""

import os, glob, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, concat, lit, window, date_format, when,
    round as sround, max as smax, min as smin
)

# ------------ CONFIG ------------
BASE_DIR = "/home/pes2ug23cs327/real_time_server_monitoring/data/processed"
NET_CSV  = f"file://{BASE_DIR}/net_data.csv"
DISK_CSV = f"file://{BASE_DIR}/disk_data.csv"

OUTPUT_DIR = "/home/pes2ug23cs327/consumer2/86_NET_DISK.csv"
FINAL_CSV_LOCAL_PATH = os.path.join(OUTPUT_DIR, "final_86_NET_DISK.csv")
TMP_DIR_LOCAL_PATH   = os.path.join(OUTPUT_DIR, "_tmp_out")
TMP_DIR_URI          = "file://" + TMP_DIR_LOCAL_PATH  # force local FS

WINDOW_SIZE = "30 seconds"
SLIDE_SIZE  = "10 seconds"

NET_IN_THRESHOLD  = 3740.67
DISK_IO_THRESHOLD = 2465.45

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------ SPARK ------------
spark = (
    SparkSession.builder
    .appName("minimal-net-disk-alerts")
    .master("local[*]")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ------------ READ ------------
net_df = (
    spark.read.option("header", True).csv(NET_CSV)
         .select("server_id", "ts", "net_in")
         .withColumn("net_in", col("net_in").cast("double"))
)

disk_df = (
    spark.read.option("header", True).csv(DISK_CSV)
         .select("server_id", "ts", "disk_io")
         .withColumn("disk_io", col("disk_io").cast("double"))
)

# ------------ JOIN ------------
joined = net_df.join(disk_df, ["server_id", "ts"], "inner")

joined = joined.withColumn(
    "event_time",
    to_timestamp(concat(lit("1970-01-01 "), col("ts")), "yyyy-MM-dd HH:mm:ss")
)

# Clamp to dataset boundaries
bounds = joined.agg(
    smin("event_time").alias("min_et"),
    smax("event_time").alias("max_et")
).collect()[0]
min_et, max_et = bounds["min_et"], bounds["max_et"]

# ------------ WINDOW + AGG ------------
win = window(col("event_time"), WINDOW_SIZE, SLIDE_SIZE)
agg = (
    joined.groupBy("server_id", win)
          .agg(
              sround(smax(col("net_in")),  2).alias("max_net_in"),
              sround(smax(col("disk_io")), 2).alias("max_disk_io"),
          )
          .where(col("window.start") >= lit(min_et))
          .where(col("window.start") <= lit(max_et))
)

with_times = agg.select(
    col("server_id"),
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"),   "HH:mm:ss").alias("window_end"),
    col("max_net_in"),
    col("max_disk_io"),
)

# ------------ ALERTS + SORT ------------
LABEL_STYLE = "possible"

NET_ONLY_LABEL = "Possible DDoS" if LABEL_STYLE == "possible" else "Network flood suspected"

out_df = (
    with_times
    .withColumn(
        "alert",
        when(
            (col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") > DISK_IO_THRESHOLD),
            lit("Network flood + Disk thrash suspected")
        )
        .when(
            (col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") <= DISK_IO_THRESHOLD),
            lit(NET_ONLY_LABEL)
        )
        .when(
            (col("max_disk_io") > DISK_IO_THRESHOLD) & (col("max_net_in") <= NET_IN_THRESHOLD),
            lit("Disk thrash suspected")
        )
        .otherwise(lit(""))
    )
    .orderBy("server_id", "window_start")
    .select("server_id", "window_start", "window_end", "max_net_in", "max_disk_io", "alert")
)

# ------------ WRITE single CSV ------------
out_df.coalesce(1).write.mode("overwrite").option("header", True).csv(TMP_DIR_URI)
part = glob.glob(os.path.join(TMP_DIR_LOCAL_PATH, "part-*.csv"))[0]
shutil.move(part, FINAL_CSV_LOCAL_PATH)
shutil.rmtree(TMP_DIR_LOCAL_PATH)
print(f"✅ Wrote: {FINAL_CSV_LOCAL_PATH}")

spark.stop()
