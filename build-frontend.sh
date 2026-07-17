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

# Build the SQL Lab frontend and update the distribution.
# Usage: ./build-frontend.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WEBAPP_DIR="$SCRIPT_DIR/exec/java-exec/src/main/resources/webapp"
DIST_BASE="$SCRIPT_DIR/distribution/target"

echo "=== Building SQL Lab frontend ==="
cd "$WEBAPP_DIR"
npm run build

echo ""
echo "=== Building java-exec module ==="
cd "$SCRIPT_DIR"
mvn package -pl exec/java-exec -DskipTests -Dcheckstyle.skip=true -q

# Find the distribution directory and copy the jar
DIST_DIR=$(find "$DIST_BASE" -name "jars" -type d 2>/dev/null | head -1)
if [ -n "$DIST_DIR" ]; then
  echo ""
  echo "=== Copying jar to distribution ==="
  cp "$SCRIPT_DIR/exec/java-exec/target/drill-java-exec-"*"-SNAPSHOT.jar" "$DIST_DIR/"
  echo "Updated: $DIST_DIR/"
else
  echo ""
  echo "WARNING: Distribution directory not found under $DIST_BASE"
  echo "Run a full 'mvn package' first to create the distribution, or copy the jar manually:"
  echo "  cp exec/java-exec/target/drill-java-exec-*-SNAPSHOT.jar <drill-distribution>/jars/"
fi

echo ""
echo "=== Done! Restart Drill and hard-refresh your browser (Cmd+Shift+R) ==="
