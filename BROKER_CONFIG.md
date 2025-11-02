# **Broker Setup Commands**

#### 1. Find the broker’s ZeroTier IP

```bash
sudo zerotier-cli listnetworks
```

#### 2. Announce the broker’s ZeroTier IP so that producer and consumer can communicate

```bash
sudo nano /opt/kafka/config/server.properties
```

Uncomment the necessary lines and paste:

```bash
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://<BROKER_ZEROTIER_IP>:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
```

#### 3. Turn off the firewall on the broker

```bash
sudo ufw disable
```

#### 4. Start Kafka and Zookeeper as daemon processes

```bash
cd /opt/kafka
sudo bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
sleep 5
sudo bin/kafka-server-start.sh -daemon config/server.properties
sleep 5
```

#### 5. Check whether Zookeeper has started

```bash
ps aux | grep zookeeper
```

Output will contain Zookeeper's process ID and configuration path.

#### 6. Check whether Kafka has started

```bash
ps aux | grep kafka | grep server.properties
```

Output will contain Kafka's process ID and configuration path.

---

### **Kafka Topic Management**

#### 7. Create all topics (run inside `/opt/kafka`)

```bash
/opt/kafka/bin/kafka-topics.sh --create --topic topic-cpu --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic topic-mem --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic topic-net --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic topic-disk --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --partitions 1 --replication-factor 1
```

#### 8. List all topics

```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server <BROKER_ZEROTIER_IP>:9092
```

#### 9. Check producer data received on each topic

```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --topic topic-cpu --from-beginning
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --topic topic-mem --from-beginning
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --topic topic-net --from-beginning
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --topic topic-disk --from-beginning
```

#### 10. Check currently subscribed consumer groups

```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server <BROKER_ZEROTIER_IP>:9092
```

#### 11. Describe and delete consumer groups

```bash
bin/kafka-consumer-groups.sh --bootstrap-server <BROKER_ZEROTIER_IP>:9092 --describe --all-groups
```

---

### **Stopping and Cleaning Up**

#### 12. Stop Kafka and Zookeeper and delete all logs

```bash
sudo pkill -f 'kafka.Kafka'
sudo pkill -f 'QuorumPeerMain'
sleep 3
ps aux | grep -E 'kafka\.Kafka|QuorumPeerMain' | grep -v grep || echo "All processes are stopped."

sudo rm -rf /tmp/kafka-logs
sudo rm -rf /tmp/zookeeper

ls /tmp | grep -E 'kafka|zookeeper' || echo "All data directories are deleted."
```

---
