import csv
import json
import time
from kafka import KafkaProducer

# --- CONFIG ---
BROKER = '192.168.195.16:9092'   # Kafka broker IP
TOPICS = {
    'cpu': 'topic-cpu',
    'mem': 'topic-mem',
    'net': 'topic-net',
    'disk': 'topic-disk'
}
CSV_FILE = '/home/pes2ug23cs382/kafka-producer/dataset.csv'  # CSV file path
# ---------------

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read CSV and send each metric to the right topic
with open(CSV_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Send CPU metric
        producer.send(TOPICS['cpu'], value={
            'timestamp': row['ts'],
            'server_id': row['server_id'],
            'cpu_pct': float(row['cpu_pct'])
        })

        # Send Memory metric
        producer.send(TOPICS['mem'], value={
            'timestamp': row['ts'],
            'server_id': row['server_id'],
            'mem_pct': float(row['mem_pct'])
        })
        # Send Network metrics (both in & out)
        producer.send(TOPICS['net'], value={
            'timestamp': row['ts'],
            'server_id': row['server_id'],
            'net_in': float(row['net_in']),
            'net_out': float(row['net_out'])
        })

        # Send Disk I/O metric
        producer.send(TOPICS['disk'], value={
            'timestamp': row['ts'],
            'server_id': row['server_id'],
            'disk_io': float(row['disk_io'])
        })

        print(f"✅ Sent metrics for {row['server_id']} at {row['ts']}")
        time.sleep(0)  # simulate real-time streaming

producer.flush()
print("🚀 All metrics sent successfully to respective topics.")


