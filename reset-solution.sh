#!/bin/bash

# Stop all containers
echo "Stopping all containers..."
docker compose down

# Start PostgreSQL, MongoDB, Zookeeper, Kafka
echo "Starting PostgreSQL, MongoDB, Zookeeper, Kafka"
docker compose up -d postgres mongodb zookeeper kafka

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Terminate active PostgreSQL connections
echo "Terminating active PostgreSQL connections..."
timeout 10s docker exec postgres psql -U postgres -d postgres -c "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'wallet_service' AND pid <> pg_backend_pid();" 2>/dev/null || echo "No active connections to terminate or command timed out"

# Clear PostgreSQL data
echo "Clearing PostgreSQL data..."
timeout 10s docker exec postgres psql -U postgres -d wallet_service -c "DROP SCHEMA IF EXISTS wallet_service CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;" 2>/dev/null || echo "Failed to clear PostgreSQL schemas, likely already clean"

# Clear MongoDB database
echo "Clearing MongoDB database..."
timeout 10s docker exec mongodb mongosh wallet_service --eval "db.dropDatabase()" 2>/dev/null || echo "Failed to drop MongoDB database, likely already clean"

sleep 30

# Delete Kafka topics
echo "Deleting Kafka topics..."
timeout 10s docker exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --delete --topic wallet-events 2>/dev/null || echo "Topic wallet-events not found, skipping..."
timeout 10s docker exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --delete --topic wallet-events-dlq 2>/dev/null || echo "Topic wallet-events-dlq not found, skipping..."

# Recreate Kafka topics
echo "Recreating Kafka topics..."
timeout 10s docker exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --create --topic wallet-events --partitions 1 --replication-factor 1
timeout 10s docker exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --create --topic wallet-events-dlq --partitions 1 --replication-factor 1

# Reset consumer group offsets
echo "Resetting consumer group offsets..."
timeout 10s docker exec kafka /usr/bin/kafka-consumer-groups --bootstrap-server kafka:9092 --group wallet-projection --reset-offsets --to-earliest --execute --topic wallet-events 2>/dev/null || echo "Consumer group wallet-projection not found, skipping..."


# Rebuild and start all services
echo "Rebuilding and starting all services..."
docker compose up --build -d

echo "Cleanup and setup completed!"