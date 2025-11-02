#!/usr/bin/env python3
"""
Consumer 2: Handles Network (max_net_in) and Disk I/O (max_disk_io) monitoring
Reads from Kafka topic, processes metrics, writes to intermediate CSV files
"""

from kafka import KafkaConsumer
import json
import csv
import os
from datetime import datetime

# ------------------ CONFIG ------------------
KAFKA_TOPICS = ['topic-net', 'topic-disk']  # Consumer 2 handles network and disk
KAFKA_BOOTSTRAP_SERVERS = ['<BROKER_IP>:9092']
CONSUMER_GROUP = 'consumer2-group'

# Paths - matching the pattern from consumer 1
BASE_DIR = "/home/pes2ug23cs327/real_time_server_monitoring/data/processed"
NET_CSV = os.path.join(BASE_DIR, "net_data.csv")
DISK_CSV = os.path.join(BASE_DIR, "disk_data.csv")

# Ensure output directory exists
os.makedirs(BASE_DIR, exist_ok=True)

def initialize_csv_files():
    """Initialize CSV files with headers if they don't exist"""
    # Network CSV
    if not os.path.exists(NET_CSV):
        with open(NET_CSV, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['server_id', 'ts', 'net_in'])
    
    # Disk CSV
    if not os.path.exists(DISK_CSV):
        with open(DISK_CSV, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['server_id', 'ts', 'disk_io'])

def process_and_write_metrics(message):
    """
    Extract network and disk metrics from Kafka message
    Write to separate CSV files based on topic
    """
    try:
        data = json.loads(message.value.decode('utf-8'))
        topic = message.topic
        
        server_id = data.get('server_id', 'unknown')
        timestamp = data.get('timestamp', '')  # Use timestamp from broker as-is, no formatting
        
        # Handle based on topic
        if topic == 'topic-net':
            net_in = float(data.get('net_in', 0.0))
            
            # Write to network CSV
            with open(NET_CSV, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([server_id, timestamp, net_in])
            
            return {
                'server_id': server_id,
                'timestamp': timestamp,
                'net_in': net_in,
                'type': 'network'
            }
        
        elif topic == 'topic-disk':
            disk_io = float(data.get('disk_io', 0.0))
            
            # Write to disk CSV
            with open(DISK_CSV, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([server_id, timestamp, disk_io])
            
            return {
                'server_id': server_id,
                'timestamp': timestamp,
                'disk_io': disk_io,
                'type': 'disk'
            }
        
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        return None

def main():
    print("=" * 60)
    print("🚀 Starting Consumer 2 - Network & Disk Monitoring")
    print("=" * 60)
    print(f"📂 Base directory: {BASE_DIR}")
    print(f"📄 Network CSV: {NET_CSV}")
    print(f"📄 Disk CSV: {DISK_CSV}")
    
    # Initialize CSV files
    initialize_csv_files()
    
    # Initialize Kafka consumer - subscribing to multiple topics
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x
    )
    
    print(f"✅ Connected to Kafka broker: {KAFKA_BOOTSTRAP_SERVERS[0]}")
    print(f"✅ Subscribed to topics: {', '.join(KAFKA_TOPICS)}")
    print("⏳ Waiting for messages...\n")
    
    message_count = 0
    net_count = 0
    disk_count = 0
    
    try:
        for message in consumer:
            # Process and write the message
            processed_data = process_and_write_metrics(message)
            
            if processed_data:
                message_count += 1
                
                if processed_data['type'] == 'network':
                    net_count += 1
                    if net_count % 10 == 0:
                        print(f"[NET {net_count}] Server: {processed_data['server_id']} | "
                              f"Time: {processed_data['timestamp']} | "
                              f"Net: {processed_data['net_in']:.2f}")
                
                elif processed_data['type'] == 'disk':
                    disk_count += 1
                    if disk_count % 10 == 0:
                        print(f"[DISK {disk_count}] Server: {processed_data['server_id']} | "
                              f"Time: {processed_data['timestamp']} | "
                              f"Disk: {processed_data['disk_io']:.2f}")
    
    except KeyboardInterrupt:
        print("\n⚠️  Consumer stopped by user")
    
    finally:
        consumer.close()
        print(f"\n📊 Total messages processed: {message_count}")
        print(f"   - Network messages: {net_count}")
        print(f"   - Disk messages: {disk_count}")
        print("👋 Consumer 2 shut down gracefully")

if __name__ == "__main__":
    main()
