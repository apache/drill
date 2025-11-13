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
