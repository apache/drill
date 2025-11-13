#!/bin/bash
# Build the Drill Hive test Docker image

set -e

echo "Building Drill Hive test Docker image..."
echo "=========================================="

cd "$(dirname "$0")"

# Build the image
docker build -t drill-hive-test:latest .

echo "=========================================="
echo "✓ Image built successfully!"
echo "  Image name: drill-hive-test:latest"
echo ""
echo "To verify the image:"
echo "  docker images drill-hive-test"
echo ""
echo "To run the container:"
echo "  docker run -d -p 9083:9083 -p 10000:10000 drill-hive-test:latest"
