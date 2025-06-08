#!/bin/bash
# Aguarda até que o broker Kafka esteja pronto
for i in {1..30}; do
  if kafka-topics --list --bootstrap-server kafka:9092 >/dev/null 2>&1; then
    echo "Kafka broker is ready"
    break
  fi
  echo "Waiting for Kafka broker... ($i/30)"
  sleep 2
done

# Cria os tópicos
kafka-topics --create --topic wallet-events --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --if-not-exists
kafka-topics --create --topic wallet-events-dlq --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --if-not-exists
kafka-topics --create --topic wallet-events-dlq-failed --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --if-not-exists

echo "Topics created or already exist"