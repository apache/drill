#!/bin/bash
# Entrypoint for pre-initialized Hive image
# Schema and data are already loaded, just start services
set -e

echo "========================================="
echo "Starting Hive services (pre-initialized)..."
echo "========================================="

export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_HEAPSIZE=2048

# Start standalone metastore in background
echo "Starting Hive Metastore on port 9083..."
/opt/hive/bin/hive --service metastore > /tmp/metastore.log 2>&1 &
METASTORE_PID=$!
echo "Metastore started (PID: $METASTORE_PID)"

# Wait for metastore to be ready (check if it's listening on port 9083)
echo "Waiting for Metastore to be ready..."
for i in {1..30}; do
    if netstat -tuln 2>/dev/null | grep -q ":9083 " || lsof -i:9083 2>/dev/null | grep -q LISTEN; then
        echo "✓ Metastore is listening on port 9083"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Metastore failed to start"
        cat /tmp/metastore.log
        exit 1
    fi
    sleep 2
done

# Start HiveServer2 in background
echo "Starting HiveServer2 on port 10000..."
/opt/hive/bin/hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &
HIVESERVER2_PID=$!
echo "HiveServer2 started (PID: $HIVESERVER2_PID)"

# Wait for HiveServer2 to accept JDBC connections
echo "Waiting for HiveServer2 to accept JDBC connections..."
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if beeline -u jdbc:hive2://localhost:10000 -e "show databases;" > /dev/null 2>&1; then
        echo "✓ HiveServer2 is ready and accepting connections!"
        break
    fi
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: HiveServer2 failed to accept connections"
        cat /tmp/hiveserver2.log
        exit 1
    fi
    echo "Waiting for HiveServer2... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

echo "========================================="
echo "Hive container ready (pre-initialized)!"
echo "Metastore: port 9083"
echo "HiveServer2: port 10000"
echo "Test data already loaded"
echo "========================================="

# Keep container running by tailing logs
tail -f /tmp/metastore.log /tmp/hiveserver2.log
