# CI/CD Integration Guide for Hive Docker Tests

This guide explains how to integrate the Hive Docker test infrastructure into your CI/CD pipelines.

## Table of Contents

- [GitHub Actions](#github-actions)
- [GitLab CI](#gitlab-ci)
- [Jenkins](#jenkins)
- [Performance Optimization](#performance-optimization)
- [Best Practices](#best-practices)

---

## GitHub Actions

### Recommended Approach: Cached Pre-initialized Image

This approach provides the best performance for CI/CD:
- **First run**: ~22 minutes (build + test)
- **Subsequent runs**: ~2.5 minutes (cached image + test)

See `.github-workflows-example.yml` for a complete working example.

**Key Benefits:**
- ✅ Fast test execution (~2 minutes with cached image)
- ✅ Reliable startup (no initialization failures)
- ✅ Cache invalidation on Docker file changes
- ✅ Cost-effective (less CI minutes used)

**How it works:**
1. Caches the pre-initialized Docker image as a tar file
2. Loads cached image on subsequent runs (30 seconds)
3. Rebuilds only when Docker files change

### Alternative: Standard Image (Simpler but Slower)

If you prefer simplicity over speed:

```yaml
- name: Build Hive Docker image
  run: |
    cd contrib/storage-hive/core/src/test/resources/docker
    docker build -t drill-hive-test:latest .

- name: Run Hive tests
  run: |
    cd contrib/storage-hive/core
    mvn test
  timeout-minutes: 35
```

**Performance:**
- **Every run**: ~19 minutes (build 2 min + initialization 17 min)
- **No caching benefit**

---

## GitLab CI

### Using GitLab's Docker-in-Docker with Caching

```yaml
hive-tests:
  image: maven:3.8-openjdk-11
  services:
    - docker:dind
  variables:
    DOCKER_HOST: tcp://docker:2376
    DOCKER_TLS_CERTDIR: "/certs"
    MAVEN_OPTS: "-Dmaven.repo.local=$CI_PROJECT_DIR/.m2/repository"

  cache:
    key: ${CI_COMMIT_REF_SLUG}
    paths:
      - .m2/repository

  before_script:
    - apt-get update && apt-get install -y docker.io

  script:
    # Check if pre-initialized image exists in registry
    - |
      if docker pull $CI_REGISTRY_IMAGE/drill-hive-test:preinitialized; then
        echo "Using cached pre-initialized image"
      else
        echo "Building new pre-initialized image"
        cd contrib/storage-hive/core/src/test/resources/docker
        ./build-preinitialized-image.sh
        docker tag drill-hive-test:preinitialized $CI_REGISTRY_IMAGE/drill-hive-test:preinitialized
        docker push $CI_REGISTRY_IMAGE/drill-hive-test:preinitialized
      fi

    # Run tests
    - cd contrib/storage-hive/core
    - mvn test -Dhive.image=$CI_REGISTRY_IMAGE/drill-hive-test:preinitialized

  timeout: 45 minutes

  artifacts:
    when: always
    paths:
      - contrib/storage-hive/core/target/surefire-reports/
    reports:
      junit: contrib/storage-hive/core/target/surefire-reports/TEST-*.xml
```

---

## Jenkins

### Using Jenkins Pipeline with Docker Registry

```groovy
pipeline {
    agent any

    environment {
        DOCKER_REGISTRY = 'your-registry.company.com'
        HIVE_IMAGE = "${DOCKER_REGISTRY}/drill-hive-test:preinitialized"
    }

    stages {
        stage('Setup') {
            steps {
                checkout scm
            }
        }

        stage('Build or Pull Hive Image') {
            steps {
                script {
                    def imageExists = sh(
                        script: "docker pull ${HIVE_IMAGE}",
                        returnStatus: true
                    ) == 0

                    if (!imageExists) {
                        echo "Building new pre-initialized Hive image"
                        dir('contrib/storage-hive/core/src/test/resources/docker') {
                            sh './build-preinitialized-image.sh'
                        }
                        sh "docker tag drill-hive-test:preinitialized ${HIVE_IMAGE}"
                        sh "docker push ${HIVE_IMAGE}"
                    } else {
                        echo "Using cached pre-initialized image"
                    }
                }
            }
        }

        stage('Run Hive Tests') {
            steps {
                dir('contrib/storage-hive/core') {
                    sh 'mvn test -Dhive.image=${HIVE_IMAGE}'
                }
            }
        }
    }

    post {
        always {
            junit 'contrib/storage-hive/core/target/surefire-reports/*.xml'
            archiveArtifacts artifacts: 'contrib/storage-hive/core/target/surefire-reports/**', allowEmptyArchive: true
        }
    }
}
```

---

## Performance Optimization

### Strategy 1: Docker Image Caching (RECOMMENDED)

**GitHub Actions:**
```yaml
- uses: actions/cache@v3
  with:
    path: /tmp/hive-image.tar
    key: drill-hive-${{ hashFiles('**/Dockerfile', '**/entrypoint.sh', '**/init-test-data.sh') }}
```

**GitLab CI:**
```yaml
# Use container registry
docker pull $CI_REGISTRY_IMAGE/drill-hive-test:preinitialized || true
docker push $CI_REGISTRY_IMAGE/drill-hive-test:preinitialized
```

**Jenkins:**
```groovy
// Use corporate Docker registry
docker.withRegistry('https://registry.company.com', 'credentials-id') {
    docker.image('drill-hive-test:preinitialized').pull()
}
```

### Strategy 2: Layer Caching

Enable Docker BuildKit for faster builds:

```yaml
env:
  DOCKER_BUILDKIT: 1
```

### Strategy 3: Parallel Test Execution

If you have multiple Hive test classes, run them in parallel:

```yaml
strategy:
  matrix:
    test:
      - TestHiveStorage
      - TestHiveViews
      - TestHivePartitions
```

---

## Best Practices

### 1. Cache Invalidation

Invalidate cache when Docker files change:

```yaml
key: hive-image-${{ hashFiles('contrib/storage-hive/**/docker/**') }}
```

### 2. Timeout Configuration

Set appropriate timeouts:

```yaml
timeout-minutes: 25  # For building pre-initialized image
timeout-minutes: 15  # For running tests with pre-initialized image
timeout-minutes: 35  # For running tests with standard image
```

### 3. Resource Limits

Ensure adequate resources for Hive:

```yaml
# GitHub Actions (ubuntu-latest has 7GB RAM, 2 CPUs - sufficient)
runs-on: ubuntu-latest

# GitLab CI
variables:
  DOCKER_MEMORY: "4g"
  DOCKER_CPUS: "2"

# Jenkins
agent {
    docker {
        image 'maven:3.8-openjdk-11'
        args '-v /var/run/docker.sock:/var/run/docker.sock --memory=4g --cpus=2'
    }
}
```

### 4. Testcontainers Configuration

Enable container reuse in CI (optional):

```yaml
- name: Enable Testcontainers reuse
  run: |
    mkdir -p ~/.testcontainers
    echo "testcontainers.reuse.enable=true" > ~/.testcontainers/testcontainers.properties
```

### 5. Cleanup

Clean up Docker resources after tests:

```yaml
post:
  always:
    - docker system prune -af --volumes || true
```

---

## Troubleshooting

### Issue: Tests timeout in CI

**Solution 1**: Increase timeout
```yaml
timeout-minutes: 45
```

**Solution 2**: Use pre-initialized image
```bash
mvn test -Dhive.image=drill-hive-test:preinitialized
```

### Issue: Out of disk space

**Solution**: Prune unused Docker images
```yaml
- name: Clean Docker
  run: docker system prune -af
```

### Issue: Container fails to start

**Solution**: Check Docker logs
```yaml
- name: Debug Hive container
  if: failure()
  run: docker logs $(docker ps -aq --filter ancestor=drill-hive-test:latest) || true
```

### Issue: Cache not working

**Solution**: Verify cache key
```yaml
- name: Debug cache
  run: |
    echo "Cache key: ${{ hashFiles('**/docker/**') }}"
    ls -la /tmp/hive-image.tar || echo "No cached image found"
```

---

## Performance Comparison

| Approach | First Run (AMD64) | Subsequent Runs | Cache Size | Notes |
|----------|-------------------|-----------------|------------|-------|
| **Cached pre-initialized** | 20 min | **2.5 min** | ~2 GB | ✅ Best for GitHub Actions |
| **Registry pre-initialized** | 20 min | **3 min** | N/A | ✅ Best for multiple repos |
| **Standard image** | 17 min | **17 min** | ~500 MB | ⚠️ No caching benefit |
| **No Docker (embedded)** | N/A | N/A | N/A | ❌ Broken on Java 11+ |

**Note**: All times are for AMD64 systems (GitHub Actions, most CI/CD). ARM64 runners would be 2-3x slower due to emulation.

**Recommendation**: Use cached pre-initialized image for best performance.

---

## Examples by CI Platform

### Minimal Example (Any CI)

```bash
# Build pre-initialized image (one-time or when Docker files change)
cd contrib/storage-hive/core/src/test/resources/docker
./build-preinitialized-image.sh

# Run tests
cd ../../..
mvn test -Dhive.image=drill-hive-test:preinitialized
```

### With Docker Registry

```bash
# Pull or build
docker pull registry.company.com/drill-hive-test:preinitialized || \
  (./build-preinitialized-image.sh && \
   docker tag drill-hive-test:preinitialized registry.company.com/drill-hive-test:preinitialized && \
   docker push registry.company.com/drill-hive-test:preinitialized)

# Run tests
mvn test -Dhive.image=registry.company.com/drill-hive-test:preinitialized
```

---

## Additional Resources

- [Testcontainers Best Practices](https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/)
- [Docker BuildKit](https://docs.docker.com/develop/develop-images/build_enhancements/)
- [GitHub Actions Caching](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows)
- [GitLab CI Docker](https://docs.gitlab.com/ee/ci/docker/using_docker_build.html)

---

## Summary

**For best CI/CD performance:**

1. ✅ Use pre-initialized image (`drill-hive-test:preinitialized`)
2. ✅ Cache the image (GitHub Actions cache or Docker registry)
3. ✅ Set appropriate timeouts (25 min first run, 15 min subsequent)
4. ✅ Monitor cache hit rates
5. ✅ Clean up Docker resources regularly

This setup reduces test time from **~19 minutes to ~2.5 minutes** on subsequent runs! 🚀
