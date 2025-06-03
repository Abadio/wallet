#!/bin/bash
set -euo pipefail

# Define output directory with timestamp
OUTPUT_DIR="logs-$(date +%Y%m%d_%H%M%S)"

# Create output directory
echo "Creating output directory: ${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}" || { echo "Failed to create directory ${OUTPUT_DIR}"; exit 1; }
chmod 755 "${OUTPUT_DIR}"

echo "Collecting logs..."

# Collect Docker container status
echo "Collecting Docker container status..."
docker ps -a > "${OUTPUT_DIR}/docker_ps.txt" 2>&1 || echo "Failed to collect docker ps" >> "${OUTPUT_DIR}/docker_ps.txt"

# Check if wallet-query-service directory exists
echo "Checking for wallet-query-service directory..."
if [ ! -d "wallet-query-service" ]; then
    echo "Error: wallet-query-service directory not found" > "${OUTPUT_DIR}/error.txt"
    tar -czf "${OUTPUT_DIR}.tar.gz" "${OUTPUT_DIR}"
    echo "Logs collected in ${OUTPUT_DIR}.tar.gz"
    exit 1
fi

# Run tests and capture logs
echo "Running Maven tests..."
cd wallet-query-service
mvn clean test -Dtest=RedisDltConsumerIntegrationTest -Dspring.profiles.active=integration -Dlogging.level.com.recargapay=TRACE > "../${OUTPUT_DIR}/test_output_logs.txt" 2>&1 || echo "Maven test execution failed" >> "../${OUTPUT_DIR}/test_output_logs.txt"
cd ..

# Filter relevant logs
echo "Filtering logs..."
if [ -f "${OUTPUT_DIR}/test_output_logs.txt" ]; then
    grep -E "com.recargapay|kafka|ERROR" "${OUTPUT_DIR}/test_output_logs.txt" > "${OUTPUT_DIR}/filtered_test_logs.txt" || echo "No relevant logs found" > "${OUTPUT_DIR}/filtered_test_logs.txt"
else
    echo "Error: test_output_logs.txt not found" > "${OUTPUT_DIR}/filtered_test_logs.txt"
fi

# Archive logs
echo "Archiving logs..."
tar -czf "${OUTPUT_DIR}.tar.gz" "${OUTPUT_DIR}" || { echo "Failed to create tar archive"; exit 1; }

# Clean up
rm -rf "${OUTPUT_DIR}"
echo "Logs collected in ${OUTPUT_DIR}.tar.gz"