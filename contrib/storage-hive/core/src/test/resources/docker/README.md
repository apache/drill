# Hive Docker Test Infrastructure

This directory contains Docker-based test infrastructure for Apache Drill's Hive storage plugin.

## Overview

The infrastructure uses [Testcontainers](https://www.testcontainers.org/) to run Apache Hive 3.1.3 in Docker containers for integration testing. This approach enables testing with Java 11+ (the embedded Hive approach only works with Java 8).

## Table of Contents

- [Quick Start](#quick-start)
- [Platform Considerations](#platform-considerations)
- [Files](#files)
- [How It Works](#how-it-works)
- [Test Data](#test-data)
- [Container Reuse](#container-reuse)
- [Performance Comparison](#performance-comparison)
- [Troubleshooting](#troubleshooting)
- [CI/CD Integration](#cicd-integration)

---

## Quick Start

### Option 1: Standard Image (Works Everywhere)

The standard image initializes schema and data on every startup. Works on all platforms but slower on first run.

```bash
# Build the standard image
cd contrib/storage-hive/core/src/test/resources/docker
docker build -t drill-hive-test:latest .

# Run tests (uses standard image by default)
cd contrib/storage-hive/core
mvn test
```

**Performance:**
- First run: 15-20 min (AMD64) or 25-35 min (ARM64)
- Subsequent runs: ~1 sec (with container reuse)

### Option 2: Pre-initialized Image (Faster, AMD64 Recommended)

The pre-initialized image has schema and data already loaded, providing startup times of ~1 minute vs 15-35 minutes.

```bash
# Build the pre-initialized image (one-time)
cd contrib/storage-hive/core/src/test/resources/docker
./build-preinitialized-image.sh

# Run tests with pre-initialized image
cd contrib/storage-hive/core
mvn test -Dhive.image=drill-hive-test:preinitialized
```

**Performance:**
- Build time: 15-20 min (AMD64) or 30-45 min (ARM64)
- First run: ~1 min
- Subsequent runs: ~1 sec (with container reuse)

---

## Platform Considerations

### AMD64 / x86_64 (Intel/AMD CPUs)
✅ **Optimal performance** - Hive Docker images are built for this architecture
- Standard image: 15-20 minutes first run
- Pre-initialized build: 15-20 minutes
- HiveServer2 startup: 5-10 minutes

### ARM64 / Apple Silicon (M1/M2/M3 Macs)
⚠️ **Slower due to emulation** - Docker must emulate AMD64 on ARM64
- Standard image: 25-35 minutes first run
- Pre-initialized build: **NOT RECOMMENDED** (may never complete)
- HiveServer2 startup: 20-30 minutes (sometimes fails entirely)

**Why so slow/unreliable?**
The official Apache Hive Docker image (`apache/hive:3.1.3`) only supports AMD64. Docker's Rosetta emulation on Apple Silicon adds 2-3x overhead, and HiveServer2 may never fully initialize due to emulation issues.

**❌ DO NOT attempt to build pre-initialized image on ARM64** - HiveServer2 often fails to start under emulation.

**✅ Recommended approaches for ARM64 developers:**
1. **Use standard image with container reuse** (slow first run ~25-35 min, then ~1 sec after)
   - First run will be slow but it works
   - Container reuse makes subsequent runs instant
   - This is the most reliable approach for ARM64
2. **Build pre-initialized image in CI/CD on AMD64** (see [CI-CD-GUIDE.md](CI-CD-GUIDE.md))
   - Push to GitHub, let Actions build on AMD64
   - Pull the built image from GHCR (see [GHCR-SETUP.md](GHCR-SETUP.md))

---

## Files

### Core Files

- **`Dockerfile`** - Base image with Apache Hive 3.1.3 and test scripts
- **`entrypoint.sh`** - Standard entrypoint that initializes schema and data on startup
- **`init-test-data.sh`** - Script that creates Hive databases, tables, and test data

### Pre-initialized Image Files

- **`build-preinitialized-image.sh`** - Script to build pre-initialized image
- **`entrypoint-preinitialized.sh`** - Fast entrypoint for pre-initialized image (no initialization)

### Build Helper

- **`build-image.sh`** - Simple script to build the base Docker image

### Documentation

- **`README.md`** - This file (overview and usage)
- **`CI-CD-GUIDE.md`** - Complete CI/CD integration guide (GitHub Actions, GitLab CI, Jenkins)
- **`GHCR-SETUP.md`** - GitHub Container Registry setup and usage
- **`.github-workflows-example.yml`** - Working GitHub Actions workflow template

---

## How It Works

### Standard Image Flow

1. Container starts from `apache/hive:3.1.3` base image
2. `entrypoint.sh` runs:
   - Initializes Derby metastore schema with `schematool`
   - Starts Hive Metastore service (port 9083)
   - Starts HiveServer2 (port 10000) - waits up to 30 minutes for readiness
   - Runs `init-test-data.sh` to create test databases and tables
   - Emits "Test data loaded and ready for queries" when complete
3. Container is ready for tests (15-35 min depending on platform)

### Pre-initialized Image Flow

1. Build script runs standard image and waits for initialization (up to 45 min)
2. Stops services cleanly with `killall java`
3. Replaces entrypoint with `entrypoint-preinitialized.sh`
4. Commits container state as new image `drill-hive-test:preinitialized`
5. On subsequent starts:
   - Schema and data already exist in committed Derby database
   - Just starts metastore and HiveServer2 services
   - Emits "Hive container ready (pre-initialized)!" when ready
   - Ready in ~1 minute!

### System Property Support

`HiveContainer.java` supports flexible image selection via system property:

```bash
# Use standard image (default)
mvn test

# Use pre-initialized image
mvn test -Dhive.image=drill-hive-test:preinitialized

# Use GHCR image
mvn test -Dhive.image=ghcr.io/apache/drill/hive-test:preinitialized
```

---

## Test Data

The following test data is created by `init-test-data.sh`:

### Databases
- `default` - Primary test database
- `db1` - Multi-database tests

### Tables in `default`

- **`kv`** - Simple key-value table (TEXT format, 5 rows)
- **`kv_parquet`** - Parquet version of kv
- **`empty_table`** - Empty table for testing
- **`readtest`** - Partitioned table with all Hive data types (TEXT format, 2 partitions)
  - Data types: BINARY, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, STRING, VARCHAR, TIMESTAMP, DATE, CHAR
  - Partitioned by: BOOLEAN, TINYINT, DECIMAL
- **`readtest_parquet`** - Parquet version of readtest with same structure
- **`infoschematest`** - Table with all Hive types for INFORMATION_SCHEMA tests
- **`hive_view`** - View over kv table

### Tables in `db1`

- **`kv_db1`** - Key-value table (3 rows)
- **`avro_table`** - Avro format table
- **`hive_view`** - View over kv_db1

---

## Container Reuse

Testcontainers is configured with `withReuse(true)` to reuse containers across test runs:

- **First run**: 15-35 minutes (or ~1 minute with pre-initialized image)
- **Subsequent runs**: ~1 second (container already running)

Enable reuse in `~/.testcontainers.properties`:
```properties
testcontainers.reuse.enable=true
```

**How it works:**
- Container keeps running after tests complete
- Next test run connects to existing container instantly
- Only reinitializes if Docker files change or container is manually removed

---

## Ports

- **9083** - Hive Metastore (Thrift protocol for Drill connections)
- **10000** - HiveServer2 (JDBC for beeline and data initialization)
- **10002** - HiveServer2 HTTP (not currently used)

---

## Rebuilding

### Rebuild Standard Image

```bash
cd contrib/storage-hive/core/src/test/resources/docker
docker build --no-cache -t drill-hive-test:latest .
```

### Rebuild Pre-initialized Image

```bash
cd contrib/storage-hive/core/src/test/resources/docker
./build-preinitialized-image.sh
```

This will:
1. Build base image if needed
2. Start container and wait for full initialization (up to 45 min)
3. Stop services cleanly
4. Commit as `drill-hive-test:preinitialized`

---

## Performance Comparison

### By Approach

| Approach | First Run | Subsequent Runs | Build Time | Platform Notes |
|----------|-----------|-----------------|------------|----------------|
| **Embedded Hive** | N/A | N/A | N/A | ❌ Broken on Java 11+ |
| **Standard Image** | 15-20 min (AMD64)<br>25-35 min (ARM64) | ~1 sec | 1-2 min | ✅ Works everywhere |
| **Pre-initialized Image** | ~1 min | ~1 sec | 15-20 min (AMD64)<br>30-45 min (ARM64) | ✅ Best for CI/CD |

### By Platform

| Platform | Standard First Run | Preinit Build | HiveServer2 Startup |
|----------|-------------------|---------------|---------------------|
| **AMD64 / x86_64** | 15-20 min | 15-20 min | 5-10 min |
| **ARM64 / M1/M2/M3** | 25-35 min | 30-45 min | 20-30 min |

**Recommendation:**
- **Local development (AMD64)**: Pre-initialized image
- **Local development (ARM64)**: Standard image with container reuse, or build pre-initialized once
- **CI/CD**: Pre-initialized image built on AMD64 and cached/pushed to registry

---

## Troubleshooting

### Container won't start

Check Docker logs:
```bash
docker ps -a | grep hive
docker logs <container-id>
```

Look for errors in:
- `/tmp/schema-init.log` - Schema initialization
- `/tmp/metastore.log` - Metastore service
- `/tmp/hiveserver2.log` - HiveServer2 service

### Tests timing out on ARM64

This is expected due to emulation. The infrastructure has been updated with longer timeouts:
- HiveServer2 wait: 30 minutes max (was 3 minutes)
- Build script: 45 minutes max (was 20 minutes)
- Testcontainers wait: 20 minutes default

If still timing out, you can increase in `HiveContainer.java`:
```java
.withStartupTimeout(Duration.ofMinutes(30))
```

Or use standard image with container reuse (slow once, fast forever).

### Pre-initialized build fails

**On ARM64**: This is often due to HiveServer2 taking longer than 30 minutes to start. The build script has been updated to wait up to 45 minutes. If it still fails:

1. Check container logs while build is running:
   ```bash
   docker logs hive-init-temp -f
   ```

2. Look for errors in HiveServer2 log:
   ```bash
   docker exec hive-init-temp tail -100 /tmp/hiveserver2.log
   ```

3. Consider using standard image instead, or build on AMD64 via CI/CD

**On AMD64**: Should complete in 15-20 minutes. If failing, check for:
- Disk space issues
- Memory constraints (needs ~4GB)
- Network issues preventing image download

### Schema errors

The Derby database is embedded in the container. If you see schema errors:
1. Remove old containers: `docker rm -f $(docker ps -aq --filter ancestor=drill-hive-test)`
2. Rebuild image: `docker build --no-cache -t drill-hive-test:latest .`

### Data not found

Ensure `HiveContainer.java` waits for the right log message:
```java
// Standard image
waitingFor(Wait.forLogMessage(".*Test data loaded and ready for queries.*", 1))

// Pre-initialized image
waitingFor(Wait.forLogMessage(".*Hive container ready \\(pre-initialized\\)!.*", 1))
```

### Container uses too much disk space

Clean up old images:
```bash
# Remove old Hive containers
docker rm -f $(docker ps -aq --filter ancestor=drill-hive-test)

# Remove unused images
docker image prune -a

# Check sizes
docker images | grep hive
```

Expected sizes:
- Base image: ~1.5 GB
- Pre-initialized image: ~2.0 GB

---

## CI/CD Integration

See [CI-CD-GUIDE.md](CI-CD-GUIDE.md) for complete integration guides for:
- GitHub Actions (with caching)
- GitLab CI
- Jenkins

### Quick Example: GitHub Actions

```yaml
name: Hive Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest  # AMD64 - fast!

    steps:
    - uses: actions/checkout@v3

    - name: Set up JDK 11
      uses: actions/setup-java@v3
      with:
        java-version: '11'
        distribution: 'temurin'
        cache: 'maven'

    - name: Cache pre-initialized Hive image
      uses: actions/cache@v3
      with:
        path: /tmp/hive-image.tar
        key: hive-${{ hashFiles('**/docker/**') }}

    - name: Load or build Hive image
      run: |
        if [ -f /tmp/hive-image.tar ]; then
          docker load -i /tmp/hive-image.tar
        else
          cd contrib/storage-hive/core/src/test/resources/docker
          ./build-preinitialized-image.sh
          docker save drill-hive-test:preinitialized -o /tmp/hive-image.tar
        fi

    - name: Run Hive tests
      run: |
        cd contrib/storage-hive/core
        mvn test -Dhive.image=drill-hive-test:preinitialized
```

See [GHCR-SETUP.md](GHCR-SETUP.md) for pushing images to GitHub Container Registry.

---

## Best Practices

### For Local Development

1. **Enable container reuse** in `~/.testcontainers.properties`
2. **First run**: Build image and run tests (slow)
3. **Subsequent runs**: Tests use existing container (~1 sec)
4. **When done**: Leave container running for next time

### For CI/CD

1. **Build pre-initialized image** on AMD64 runner
2. **Cache or push to registry** (GHCR, Docker Hub, etc.)
3. **Pull cached image** in test jobs (~3 min vs 20 min rebuild)
4. **Run tests** with pre-initialized image

### When to Rebuild

Rebuild images when:
- Docker files change (`Dockerfile`, `entrypoint.sh`, `init-test-data.sh`)
- Test data requirements change
- Upgrading Hive version
- Schema changes

---

## Architecture Notes

### Why Two Services?

Apache Drill connects to the **Hive Metastore** (port 9083) via Thrift protocol, not HiveServer2. However, we need **HiveServer2** (port 10000) running to:
1. Initialize test data via beeline (JDBC)
2. Run queries during initialization
3. Fully validate that Hive is operational

Both services share the same Derby database, so data created via HiveServer2 is visible to Drill via Metastore.

### Why Derby?

Derby is an embedded Java database suitable for testing. For production Hive deployments, you would use:
- MySQL/MariaDB
- PostgreSQL
- Oracle

But Derby is perfect for lightweight test containers.

---

## License

Licensed under the Apache License, Version 2.0. See the NOTICE file distributed with Apache Drill.
