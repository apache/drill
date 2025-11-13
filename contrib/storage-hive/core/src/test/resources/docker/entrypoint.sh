#!/bin/bash
# Custom entrypoint that starts both metastore and HiveServer2
set -e

echo "========================================="
echo "Starting Hive services..."
echo "========================================="

export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_HEAPSIZE=2048

# Initialize Hive metastore schema
echo "Initializing Hive schema..."
/opt/hive/bin/schematool -dbType derby -initSchema > /tmp/schema-init.log 2>&1 || echo "Schema already initialized"

# Start standalone metastore in background
echo "Starting Hive Metastore on port 9083..."
/opt/hive/bin/hive --service metastore > /tmp/metastore.log 2>&1 &
METASTORE_PID=$!
echo "Metastore started (PID: $METASTORE_PID)"

# Wait for metastore to be ready (simple time-based wait + log check)
echo "Waiting for Metastore to be ready..."
sleep 30
if grep -q "Starting Hive Metastore Server" /tmp/metastore.log; then
    echo "✓ Metastore is starting"
else
    echo "ERROR: Metastore failed to start"
    cat /tmp/metastore.log
    exit 1
fi

# Start HiveServer2 in background
echo "Starting HiveServer2 on port 10000..."
/opt/hive/bin/hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &
HIVESERVER2_PID=$!
echo "HiveServer2 started (PID: $HIVESERVER2_PID)"

# Wait for HiveServer2 to accept JDBC connections
echo "Waiting for HiveServer2 to accept JDBC connections..."
echo "This may take 5-10 minutes (AMD64) or 20-30 minutes (ARM64)..."
MAX_RETRIES=360  # 360 attempts × 5 seconds = 30 minutes max
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if beeline -u jdbc:hive2://localhost:10000 -e "show databases;" > /dev/null 2>&1; then
        echo "✓ HiveServer2 is ready and accepting connections!"
        break
    fi
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: HiveServer2 failed to accept connections after 30 minutes"
        cat /tmp/hiveserver2.log
        exit 1
    fi
    # Show progress every minute
    if [ $((RETRY_COUNT % 12)) -eq 0 ]; then
        echo "Waiting for HiveServer2... ($((RETRY_COUNT / 12)) minutes elapsed)"
    fi
    sleep 5
done

# Run test data initialization
echo "========================================="
echo "Initializing test data..."
echo "========================================="
if [ -f /tmp/init-test-data.sh ]; then
    /tmp/init-test-data.sh
    if [ $? -eq 0 ]; then
        echo "✓ Test data initialized successfully"
    else
        echo "ERROR: Test data initialization failed"
        exit 1
    fi
else
    echo "WARNING: init-test-data.sh not found"
fi

echo "========================================="
echo "Hive container ready!"
echo "Metastore: port 9083"
echo "HiveServer2: port 10000"
echo "Test data loaded and ready for queries"
echo "========================================="

# Keep container running by tailing logs
tail -f /tmp/metastore.log /tmp/hiveserver2.log
