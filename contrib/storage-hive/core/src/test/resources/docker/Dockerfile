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

# Dockerfile for Apache Drill Hive Test Container
# Extends official Apache Hive image with pre-populated test data
FROM apache/hive:3.1.3

LABEL maintainer="Apache Drill"
LABEL description="Hive 3.1.3 with Apache Drill test data pre-loaded"

# Create test data directory
RUN mkdir -p /tmp/drill-hive-test-data

# Copy initialization script that will create test tables and data
COPY --chmod=755 init-test-data.sh /tmp/init-test-data.sh

# Copy custom entrypoint that starts services and initializes data
COPY --chmod=755 entrypoint.sh /opt/entrypoint.sh

# Expose Hive ports
EXPOSE 9083 10000 10002

# Use custom entrypoint
ENTRYPOINT ["/opt/entrypoint.sh"]
