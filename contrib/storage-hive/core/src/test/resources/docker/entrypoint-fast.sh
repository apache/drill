#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Fast entrypoint that skips test data initialization
# Test data will be created via JDBC in @BeforeClass methods
set -e

echo "========================================="
echo "Starting Hive services (FAST MODE)..."
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
echo "This should take 1-3 minutes..."
MAX_RETRIES=60  # 60 attempts × 5 seconds = 5 minutes max
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if beeline -u jdbc:hive2://localhost:10000 -e "show databases;" > /dev/null 2>&1; then
        echo "✓ HiveServer2 is ready and accepting connections!"
        break
    fi
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: HiveServer2 failed to accept connections after 5 minutes"
        cat /tmp/hiveserver2.log
        exit 1
    fi
    # Show progress every 30 seconds
    if [ $((RETRY_COUNT % 6)) -eq 0 ]; then
        echo "Waiting for HiveServer2... ($((RETRY_COUNT * 5)) seconds elapsed)"
    fi
    sleep 5
done

echo "========================================="
echo "Hive container ready (FAST MODE)!"
echo "Metastore: port 9083"
echo "HiveServer2: port 10000"
echo "NOTE: Test data will be created by tests via JDBC"
echo "Test data loaded and ready for queries"
echo "========================================="

# Keep container running by tailing logs
tail -f /tmp/metastore.log /tmp/hiveserver2.log
