import time
from kafka import KafkaConsumer
import csv
import json
import os
from datetime import datetime

# ------------------ CONFIG ------------------
BROKER = "<BROKER_IP>:9092"
TOPICS = ["topic-cpu", "topic-mem"]
PROCESSED_DIR = "/home/pes2ug23cs313/real_time_server_monitoring/data/processed"

CPU_CSV = os.path.join(PROCESSED_DIR, "cpu_data.csv")
MEM_CSV = os.path.join(PROCESSED_DIR, "mem_data.csv")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ------------------ INIT CSV ------------------
def init_csv(file_path, headers):
    """Creates a CSV file with headers if it doesn't exist."""
    if not os.path.exists(file_path):
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

init_csv(CPU_CSV, ["ts", "server_id", "cpu_pct"])
init_csv(MEM_CSV, ["ts", "server_id", "mem_pct"])

# ------------------ KAFKA CONSUMER ------------------
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # read from beginning if no offset exists
    enable_auto_commit=True,
    group_id=f"consumer1-cpu-mem-{int(time.time())}"  # unique per run
)

print("\u2705 Consumer started. Press Ctrl+C to stop.")

try:
    with open(CPU_CSV, "a", newline="") as cpu_file, \
         open(MEM_CSV, "a", newline="") as mem_file:

        cpu_writer = csv.writer(cpu_file)
        mem_writer = csv.writer(mem_file)

        for message in consumer:
            data = message.value
            topic = message.topic
            print(f"Received on {topic}: {data}")

            # ------------------ TIMESTAMP ------------------
            ts = data.get("ts") or data.get("timestamp")
            if not ts:
                ts = datetime.now().isoformat()  # fallback if broker didn't send ts

            server_id = data.get("server_id", "unknown")

            # ------------------ WRITE TO CSV ------------------
            if topic == "topic-cpu":
                cpu_pct = data.get("cpu_pct", 0.0)
                cpu_writer.writerow([ts, server_id, cpu_pct])
                cpu_file.flush()

            elif topic == "topic-mem":
                mem_pct = data.get("mem_pct", 0.0)
                mem_writer.writerow([ts, server_id, mem_pct])
                mem_file.flush()

except KeyboardInterrupt:
    print("\n\U0001f6d1 Consumer stopped by user.")
finally:
    consumer.close()
    print("\u2705 Consumer connection closed.")

