# Real-Time Server Monitoring Pipeline
## Overview
Kafka-based streaming system for server metrics (CPU, Memory, Network, Disk I/O). Producers send data to topics; consumers write to CSVs; Spark jobs process for 30s tumbling windows (10s slide) and generate alerts.

## Architecture
Producer: Reads dataset.csv, publishes to topic-cpu, topic-mem, topic-net, topic-disk.
Consumer 1: Subscribes to CPU/Mem, writes to cpu_data.csv/mem_data.csv.
Consumer 2: Subscribes to Net/Disk, writes to net_data.csv/disk_data.csv.
Spark Job 1: Joins CPU/Mem CSVs, averages over windows, alerts on thresholds (CPU>81.69%, Mem>76.53%). Output: team_001_CPU_MEM.csv.
Spark Job 2: Joins Net/Disk CSVs, maxes over windows, alerts on thresholds (NetIn>3740.67, DiskIO>2465.45). Output: final_86_NET_DISK.csv.

## Prerequisites
Kafka (broker: <broker_ip>:9092).
PySpark (Python 3).
Dataset: dataset.csv with columns ts, server_id, cpu_pct, mem_pct, net_in, net_out, disk_io.

## Running
Producer: python producer.py (sends all rows).
```bash
python3 producer.py
```
Consumer 1: python consumer1_kafka.py (Ctrl+C to stop).
```bash
python3 consumer1_kafka.py
```
Consumer 2: python consumer2_kafka.py (Ctrl+C to stop).
```bash
python3 consumer2_kafka.py
```
Spark Job 1: python consumer1_spark_job.py (after CSVs populated).
```bash
spark-submit consumer1_spark_job.py
```
Spark Job 2: python consumer2_spark_job.py (after CSVs populated).
```bash
spark-submit consumer2_spark_job.py
```
