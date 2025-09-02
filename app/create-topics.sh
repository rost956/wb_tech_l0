#!/bin/sh
# Wait for Kafka to be ready
sleep 10

# Create topic
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic orders

echo "Topic orders created"