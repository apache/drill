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
