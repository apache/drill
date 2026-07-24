# Docker-Based Hive Test Infrastructure

This document describes the new Docker-based Hive test infrastructure for Apache Drill, which replaces the embedded Hive approach.

## Overview

The Hive storage plugin tests now use Docker containers via Testcontainers instead of embedded Hive instances. This provides:

- **Java 11+ Compatibility**: No longer limited to Java 8
- **Real Hive Environment**: Tests run against actual Hive 3.1.3
- **Better Performance**: Container reuse across tests
- **CI/CD Ready**: Works in containerized build environments

## Architecture

### Components

1. **`HiveContainer`** - Testcontainers wrapper for Hive
   - Singleton pattern ensures one container for all tests
   - Auto-starts on first test, reused for subsequent tests
   - Exposes ports: 9083 (metastore), 10000 (HiveServer2)

2. **`drill-hive-test` Docker Image** - Custom Hive image with test data
   - Based on `apache/hive:3.1.3`
   - Pre-loads test databases and tables on startup
   - Located in: `src/test/resources/docker/`

3. **`HiveTestBase`** - Updated base test class
   - Removed Java 8 version checks
   - Connects to Docker-based Hive
   - All existing tests extend this class

## Setup Instructions

### 1. Build the Docker Image

**First time only** - Build the custom Hive image with test data:

```bash
cd contrib/storage-hive/core/src/test/resources/docker
./build-image.sh
```

Or manually:
```bash
docker build -t drill-hive-test:latest .
```

### 2. Run Tests

```bash
cd contrib/storage-hive/core
mvn test
```

## Test Data

The Docker image includes these pre-loaded tables:

### Databases
- `default` - Default Hive database
- `db1` - Secondary database for multi-DB tests

### Tables
- **default.kv** - Simple key-value table (5 rows)
- **db1.kv_db1** - Key-value in separate database
- **default.empty_table** - Empty table for edge case testing
- **default.readtest** - Comprehensive data types table (partitioned)
  - All Hive primitive types
  - 2 partitions with different tinyint_part values
- **default.infoschematest** - All Hive types including complex types
- **default.kv_parquet** - Parquet format table
- **default.readtest_parquet** - Readtest in Parquet format

### Views
- **default.hive_view** - View on kv table
- **db1.hive_view** - View on kv_db1 table

## Performance

| Scenario | Time |
|----------|------|
| First test (cold start) | 2-5 minutes |
| Subsequent tests | <1 second |
| Container startup (one time) | ~90 seconds |
| Test data initialization | ~60 seconds |

The container is reused across all test classes, so the startup cost is paid only once per Maven execution.

## How It Works

### Test Execution Flow

1. First test class loads → `HiveTestBase` static initializer runs
2. `HiveContainer.getInstance()` starts Docker container (if not already running)
3. Container starts Hive services (metastore + HiveServer2)
4. `init-test-data.sh` creates all test databases and tables
5. Drill connects to containerized Hive via Thrift
6. Tests run against the container
7. Subsequent test classes reuse the same container (fast!)
8. Container cleaned up at JVM shutdown by Testcontainers

### Key Files

```
contrib/storage-hive/core/
├── src/test/java/org/apache/drill/exec/hive/
│   ├── HiveContainer.java          # Testcontainers wrapper
│   ├── HiveTestBase.java           # Base class for all Hive tests
│   ├── HiveTestFixture.java        # Configuration builder
│   └── HiveTestSuite.java          # Suite runner
└── src/test/resources/docker/
    ├── Dockerfile                  # Custom Hive image definition
    ├── init-test-data.sh           # Test data initialization script
    ├── build-image.sh              # Helper script to build image
    ├── README.md                   # Docker-specific documentation
    └── test-data/                  # Test data files
        └── kv_data.txt
```

## Troubleshooting

### Docker Image Not Found
```
Error: Unable to find image 'drill-hive-test:latest' locally
```
**Solution**: Build the Docker image first (see Setup Instructions)

### Container Won't Start
```
Hive container failed to start within timeout
```
**Solutions**:
- Ensure Docker is running
- Check available disk space (image is ~900MB)
- Increase timeout in `HiveContainer.java` if on slow systems

### Port Already in Use
```
Bind for 0.0.0.0:9083 failed: port is already allocated
```
**Solution**: Stop any running Hive containers or services using ports 9083, 10000, 10002

### Tests Failing After Changes
If you modified test data requirements:
1. Update `init-test-data.sh` with new tables/data
2. Rebuild the Docker image: `./build-image.sh`
3. Restart tests

## Extending Test Data

To add new test tables:

1. Edit `src/test/resources/docker/init-test-data.sh`
2. Add your CREATE TABLE and INSERT statements
3. Rebuild the image: `./build-image.sh`
4. Run tests

Example:
```sql
-- In init-test-data.sh
CREATE TABLE IF NOT EXISTS my_test_table(
  id INT,
  name STRING
);

INSERT INTO my_test_table VALUES (1, 'test'), (2, 'data');
```

## Migration Notes

### Removed Components
- ❌ `HiveTestUtilities.supportedJavaVersion()` - No longer needed
- ❌ `HiveTestUtilities.assumeJavaVersion()` - No longer needed
- ❌ Java 8 version checks in all test classes
- ❌ Embedded Derby metastore configuration
- ❌ `HiveDriverManager` usage in `HiveTestBase` (temporarily disabled)

### Updated Components
- ✅ `HiveTestBase` - Uses Docker container
- ✅ `HiveTestFixture` - Added `builderForDocker()` method
- ✅ `HiveClusterTest` - Removed Java version check

## Future Enhancements

Potential improvements:

1. **Complete Test Data**: Port all `HiveTestDataGenerator` logic to init script
2. **Test Resource Files**: Copy Avro schemas, JSON files into image
3. **ORC Tables**: Add more ORC format tables for filter pushdown tests
4. **Complex Types**: Add comprehensive array/map/struct test data
5. **Partition Pruning**: Add more partitioned tables for optimization tests
6. **Performance**: Optimize container startup with custom entrypoint

## CI/CD Integration

The Docker-based tests work seamlessly in CI/CD:

```yaml
# Example GitHub Actions
- name: Run Hive Tests
  run: |
    cd contrib/storage-hive/core/src/test/resources/docker
    ./build-image.sh
    cd ../../../..
    mvn test -Dtest=*Hive*
```

Testcontainers automatically handles Docker-in-Docker scenarios.

## Support

For issues or questions:
- Check logs: Container logs are visible in test output
- Debug mode: Set `-X` flag in Maven for verbose output
- Container inspection: `docker ps` and `docker logs <container-id>`
