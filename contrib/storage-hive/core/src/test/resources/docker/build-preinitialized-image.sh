#!/bin/bash
# Script to build a pre-initialized Hive Docker image
# This runs a container, initializes schema and data, then commits as a new image
# The resulting image starts much faster (~1 minute vs 15+ minutes)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_IMAGE="drill-hive-test:latest"
PREINITIALIZED_IMAGE="drill-hive-test:preinitialized"
TEMP_CONTAINER="hive-init-temp"

echo "=========================================="
echo "Building Pre-initialized Hive Image"
echo "=========================================="
echo "This will take:"
echo "  - 15-20 minutes on AMD64/x86_64 systems"
echo "  - 30-45 minutes on ARM64/M1/M2 Macs (due to emulation)"
echo "The resulting image starts in ~1 minute"
echo "=========================================="

# Step 1: Build the base image if it doesn't exist
if ! docker image inspect "$BASE_IMAGE" > /dev/null 2>&1; then
    echo "Building base image: $BASE_IMAGE"
    cd "$SCRIPT_DIR"
    docker build -t "$BASE_IMAGE" .
else
    echo "Base image $BASE_IMAGE already exists"
fi

# Step 2: Clean up any existing temporary container
echo "Cleaning up any existing temporary containers..."
docker rm -f "$TEMP_CONTAINER" 2>/dev/null || true

# Step 3: Start container and let it fully initialize
echo "=========================================="
echo "Starting container for initialization..."
echo "This will take 15-20 minutes (AMD64) or 30-45 minutes (ARM64)..."
echo "=========================================="

# Run the container in detached mode
docker run -d \
    --name "$TEMP_CONTAINER" \
    "$BASE_IMAGE"

# Wait for the "Test data loaded and ready for queries" message
echo "Waiting for initialization to complete..."
MAX_WAIT=2700  # 45 minutes (to accommodate ARM64 emulation)
ELAPSED=0
INTERVAL=10

while [ $ELAPSED -lt $MAX_WAIT ]; do
    if docker logs "$TEMP_CONTAINER" 2>&1 | grep -q "Test data loaded and ready for queries"; then
        echo "✓ Initialization complete!"
        break
    fi

    # Show progress
    if [ $((ELAPSED % 60)) -eq 0 ]; then
        echo "Waiting... ($((ELAPSED / 60)) minutes elapsed)"
    fi

    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "ERROR: Initialization timed out after 45 minutes"
    echo "Container logs:"
    docker logs "$TEMP_CONTAINER" 2>&1 | tail -100
    docker rm -f "$TEMP_CONTAINER"
    exit 1
fi

# Step 4: Stop the services cleanly
echo "Stopping services cleanly..."
docker exec "$TEMP_CONTAINER" killall java 2>/dev/null || true
sleep 5

# Step 5: Replace entrypoint with the fast-start version
echo "Updating entrypoint for fast startup..."
docker cp "$SCRIPT_DIR/entrypoint-preinitialized.sh" "$TEMP_CONTAINER:/opt/entrypoint.sh"
docker exec "$TEMP_CONTAINER" chmod +x /opt/entrypoint.sh

# Step 6: Commit the container as a new image
echo "Committing container as pre-initialized image..."
docker commit \
    --change='ENTRYPOINT ["/opt/entrypoint.sh"]' \
    --change='EXPOSE 9083 10000 10002' \
    --message "Pre-initialized Hive with test data loaded" \
    "$TEMP_CONTAINER" \
    "$PREINITIALIZED_IMAGE"

# Step 7: Clean up temporary container
echo "Cleaning up temporary container..."
docker rm -f "$TEMP_CONTAINER"

echo "=========================================="
echo "✓ Pre-initialized image built successfully!"
echo "Image: $PREINITIALIZED_IMAGE"
echo "Startup time: ~1 minute (vs 15+ minutes)"
echo "=========================================="
echo ""
echo "To use this image, update HiveContainer.java:"
echo "  private static final String HIVE_IMAGE = \"$PREINITIALIZED_IMAGE\";"
echo ""
echo "To rebuild, run: $0"
echo "=========================================="
