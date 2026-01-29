# Hive Storage Plugin Testing Guide

## Overview

The Hive storage plugin has two types of tests:

### Unit Tests (Always Run - No Hive Required)
These tests run on all architectures without requiring a Hive connection:
- **TestSchemaConversion** (9 tests) - Hive→Drill type conversion logic
- **TestColumnListCache** (5 tests) - Column list caching logic
- **SkipFooterRecordsInspectorTest** (2 tests) - Record skipping logic with mocks

### Integration Tests (Require Docker Hive)
The Hive storage plugin integration tests use Docker containers to provide a real Hive metastore and HiveServer2 environment. This approach is necessary because:

1. **Java 11+ Compatibility**: Embedded HiveServer2 mode is deprecated and incompatible with Java 11+
2. **Real Integration Testing**: Docker provides authentic Hive behavior for complex type testing
3. **Official Recommendation**: Apache Hive project recommends Docker for testing

## Architecture Considerations

### ARM64 (Apple Silicon) Limitation

The Hive Docker image (`apache/hive:3.1.3`) is AMD64-only. On ARM64 Macs:
- Docker uses Rosetta 2 emulation
- Container startup takes **20-30 minutes** on first run
- Subsequent runs are fast due to container reuse (~1 second)

### AMD64 Performance

On AMD64 architecture (Intel/AMD processors, most CI/CD):
- Container startup takes **1-3 minutes** on first run
- Fast enough for local development and CI/CD

## Solution Options

### Option 1: Skip Tests on ARM64 (RECOMMENDED for local development)

Tests are automatically skipped on ARM64 but run normally in CI/CD on AMD64.

```bash
# On ARM64 Mac - Hive tests are skipped automatically
mvn test

# Force run Hive tests even on ARM64 (expect 20-30 min first startup)
mvn test -Pforce-hive-tests
```

### Option 2: Pre-start Container (for ARM64 development)

Start the container once and keep it running for the day:

```bash
# Start container (takes 20-30 minutes first time, ~1 second if reused)
docker run -d --name hive-dev \
  -p 9083:9083 -p 10000:10000 -p 10002:10002 \
  drill-hive-test:fast

# Wait for container to be ready (check logs)
docker logs -f hive-dev

# Run tests (they'll connect to existing container)
mvn test -Pforce-hive-tests

# Stop container at end of day
docker stop hive-dev
```

### Option 3: Use AMD64 Environment

Run tests on AMD64 hardware or CI/CD where Docker performance is good:

- GitHub Actions (ubuntu-latest)
- GitLab CI (linux/amd64)
- Jenkins on AMD64 nodes
- Cloud VM with AMD64 processor

## Test Categories

All Hive integration tests are tagged with `@Category(HiveStorageTest.class)`:

```java
@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveMaps extends HiveTestBase {
    // Tests for Hive MAP types
}
```

The six main complex type test classes:
1. **TestHiveArrays** - Hive ARRAY types (52 test methods)
2. **TestHiveMaps** - Hive MAP types
3. **TestHiveStructs** - Hive STRUCT types
4. **TestHiveUnions** - Hive UNION types
5. **TestStorageBasedHiveAuthorization** - Storage-based auth
6. **TestSqlStdBasedAuthorization** - SQL standard auth

## Docker Images

### Fast Image (Default - 1-3 min startup)

Used by default. Test data created by tests via JDBC:

```bash
# Build fast image
cd src/test/resources/docker
docker build -f Dockerfile.fast -t drill-hive-test:fast .
```

### Pre-initialized Image (1 min startup)

Contains pre-loaded test data. Build with:

```bash
cd src/test/resources/docker
./build-preinitialized-image.sh
```

Use with:
```bash
mvn test -Dhive.image=drill-hive-test:preinitialized
```

## Customization

### Use Different Hive Image

```bash
# Use custom image
mvn test -Dhive.image=my-hive-image:tag

# Use official Hive image directly
mvn test -Dhive.image=apache/hive:3.1.3
```

### Increase Startup Timeout

If container startup is slow, increase timeout in HiveContainer.java:

```java
waitingFor(Wait.forLogMessage(".*ready.*", 1)
    .withStartupTimeout(Duration.ofMinutes(30))); // Increase from 20
```

## Troubleshooting

### Tests Fail with "NoClassDefFoundError: HiveTestBase"

**Cause**: Container startup timeout during static initialization

**Solution**:
1. Pre-start container (see Option 2 above)
2. Use AMD64 environment
3. Skip tests on ARM64 (default behavior)

### Container Startup Takes Forever

**Cause**: ARM64 emulation

**Check architecture**:
```bash
uname -m  # aarch64 = ARM64, x86_64 = AMD64
```

**Solutions**: See Option 1, 2, or 3 above

### Tests Pass Locally but Fail in CI

**Cause**: Different architecture or Docker configuration

**Solution**: Ensure CI uses AMD64 runners and has Docker access

### Need to Debug Hive Setup

```bash
# Connect to running container
docker exec -it hive-dev /bin/bash

# Check Hive services
docker exec -it hive-dev ps aux | grep hive

# View logs
docker logs hive-dev

# Test JDBC connection
docker exec -it hive-dev beeline -u jdbc:hive2://localhost:10000 -e "show databases;"
```

## CI/CD Configuration

### GitHub Actions Example

```yaml
name: Hive Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest  # AMD64 architecture

    steps:
    - uses: actions/checkout@v3

    - name: Set up JDK 11
      uses: actions/setup-java@v3
      with:
        java-version: '11'

    - name: Run Hive tests
      run: mvn test -pl contrib/storage-hive/core -Pforce-hive-tests
      timeout-minutes: 30  # Allow time for first container start
```

## Summary

- **All 6 complex type tests are fully functional** and compile with zero @Ignore annotations
- **Tests work great on AMD64** (1-3 min startup)
- **Tests auto-skip on ARM64** due to 20-30 min Docker emulation penalty
- **Force-run on ARM64** with `-Pforce-hive-tests` if needed (expect slow first run)
- **CI/CD on AMD64** runs tests normally with good performance
- **Embedded HiveServer2 is not an option** - deprecated by Apache Hive for Java 11+

The Docker approach is the correct and officially recommended solution. The ARM64 limitation is a Docker/architecture issue, not a problem with the test design.
