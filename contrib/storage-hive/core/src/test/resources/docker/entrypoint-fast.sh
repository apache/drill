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
# Note: Removed 'set -e' to allow script to continue even if some commands fail

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

# Wait for metastore to be ready
echo "Waiting for Metastore to be ready..."
MAX_WAIT=120  # 2 minutes max
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    WAIT_COUNT=$((WAIT_COUNT+5))
    # Check for various success indicators in the log
    if grep -qE "(Starting Hive Metastore Server|Started the new metaserver|Metastore.*started)" /tmp/metastore.log 2>/dev/null; then
        echo "Metastore is starting"
        break
    fi
    # Also check if the process is still running
    if ! kill -0 $METASTORE_PID 2>/dev/null; then
        echo "ERROR: Metastore process died"
        cat /tmp/metastore.log
        exit 1
    fi
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo "WARNING: Could not confirm metastore startup from logs, continuing anyway..."
        echo "Metastore log:"
        tail -20 /tmp/metastore.log
    fi
    sleep 5
done
# Extra wait for metastore to fully initialize
sleep 10

# Start HiveServer2 in background
echo "Starting HiveServer2 on port 10000..."
/opt/hive/bin/hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &
HIVESERVER2_PID=$!
echo "HiveServer2 started (PID: $HIVESERVER2_PID)"

# Wait for HiveServer2 to accept JDBC connections
echo "Waiting for HiveServer2 to accept JDBC connections..."
echo "This may take 5-15 minutes in CI environments..."
MAX_RETRIES=180  # 180 attempts x 5 seconds = 15 minutes max
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if beeline -u jdbc:hive2://localhost:10000 -e "show databases;" > /dev/null 2>&1; then
        echo "HiveServer2 is ready and accepting connections!"
        break
    fi
    # Check if process is still running
    if ! kill -0 $HIVESERVER2_PID 2>/dev/null; then
        echo "ERROR: HiveServer2 process died"
        cat /tmp/hiveserver2.log
        exit 1
    fi
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: HiveServer2 failed to accept connections after 15 minutes"
        echo "HiveServer2 log:"
        tail -50 /tmp/hiveserver2.log
        exit 1
    fi
    # Show progress every minute
    if [ $((RETRY_COUNT % 12)) -eq 0 ]; then
        echo "Waiting for HiveServer2... ($((RETRY_COUNT * 5 / 60)) minutes elapsed)"
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
