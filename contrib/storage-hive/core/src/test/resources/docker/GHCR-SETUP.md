# GitHub Container Registry Setup Guide

This guide shows you how to push the pre-initialized Hive Docker image to GitHub Container Registry (GHCR) for use in CI/CD.

## Table of Contents

- [Why Use GHCR?](#why-use-ghcr)
- [One-Time Setup](#one-time-setup)
- [Manual Push](#manual-push)
- [Automated CI/CD Push](#automated-cicd-push)
- [Using the Image in Tests](#using-the-image-in-tests)

---

## Why Use GHCR?

**Benefits:**
- ✅ Free for public repositories
- ✅ No need to cache tar files in GitHub Actions
- ✅ Faster pulls than building from scratch
- ✅ Shared across all CI runs and developers
- ✅ Automatic cleanup of old images

**Performance:**
- First build + push: ~25 minutes (one-time)
- CI runs pulling from GHCR: ~3 minutes (vs 2.5 min with Actions cache, but more reliable)
- No cache storage limits

---

## One-Time Setup

### Step 1: Create a Personal Access Token (PAT)

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
   - Or visit: https://github.com/settings/tokens

2. Click **"Generate new token (classic)"**

3. Configure the token:
   - **Note**: `Drill Hive Docker Image Push`
   - **Expiration**: 90 days (or No expiration for long-term use)
   - **Scopes**: Check these boxes:
     - ✅ `write:packages` (includes read:packages)
     - ✅ `delete:packages` (optional, for cleanup)
     - ✅ `repo` (if repository is private)

4. Click **"Generate token"** and **copy the token** (you won't see it again!)

### Step 2: Authenticate Docker with GHCR

```bash
# Save your token to a file (more secure than typing)
echo "YOUR_GITHUB_TOKEN" > ~/.github-token
chmod 600 ~/.github-token

# Login to GHCR
cat ~/.github-token | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin

# Verify login
docker info | grep -A 5 "Registry:"
```

**Security Note:** Never commit your token to Git! Add `~/.github-token` to `.gitignore`.

---

## Manual Push

### Option 1: Push Pre-initialized Image (Recommended)

This is the fastest approach for CI/CD.

```bash
# Step 1: Build the pre-initialized image locally
cd /Users/charlesgivre/github/drill/contrib/storage-hive/core/src/test/resources/docker
./build-preinitialized-image.sh

# Step 2: Tag for GHCR
# Format: ghcr.io/OWNER/REPO/IMAGE:TAG
# Example: ghcr.io/apache/drill/hive-test:preinitialized
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:preinitialized

# Step 3: Push to GHCR
docker push ghcr.io/apache/drill/hive-test:preinitialized

# Step 4: (Optional) Also tag as 'latest'
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:latest
docker push ghcr.io/apache/drill/hive-test:latest
```

**Expected output:**
```
The push refers to repository [ghcr.io/apache/drill/hive-test]
...
preinitialized: digest: sha256:abc123... size: 4321
```

### Option 2: Push Standard Image Only

If you want to build in CI each time:

```bash
# Build and push standard image
cd /Users/charlesgivre/github/drill/contrib/storage-hive/core/src/test/resources/docker
docker build -t ghcr.io/apache/drill/hive-test:standard .
docker push ghcr.io/apache/drill/hive-test:standard
```

---

## Automated CI/CD Push

### GitHub Actions Workflow (Recommended)

This automatically builds and pushes the image when Docker files change.

Create `.github/workflows/build-hive-image.yml`:

```yaml
name: Build and Push Hive Docker Image

on:
  push:
    branches: [ master, main ]
    paths:
      - 'contrib/storage-hive/core/src/test/resources/docker/**'
  workflow_dispatch:  # Allow manual trigger

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write  # Required for GHCR push

    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Log in to GitHub Container Registry
      uses: docker/login-action@v2
      with:
        registry: ghcr.io
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}  # Automatically provided by GitHub

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v2

    - name: Build standard Hive image
      run: |
        cd contrib/storage-hive/core/src/test/resources/docker
        docker build -t drill-hive-test:latest .

    - name: Build pre-initialized Hive image
      run: |
        cd contrib/storage-hive/core/src/test/resources/docker
        ./build-preinitialized-image.sh
      timeout-minutes: 25

    - name: Tag images for GHCR
      run: |
        # Tag standard image
        docker tag drill-hive-test:latest ghcr.io/${{ github.repository_owner }}/drill-hive-test:standard

        # Tag pre-initialized image
        docker tag drill-hive-test:preinitialized ghcr.io/${{ github.repository_owner }}/drill-hive-test:preinitialized
        docker tag drill-hive-test:preinitialized ghcr.io/${{ github.repository_owner }}/drill-hive-test:latest

    - name: Push images to GHCR
      run: |
        docker push ghcr.io/${{ github.repository_owner }}/drill-hive-test:standard
        docker push ghcr.io/${{ github.repository_owner }}/drill-hive-test:preinitialized
        docker push ghcr.io/${{ github.repository_owner }}/drill-hive-test:latest

    - name: Image information
      run: |
        echo "Images pushed to:"
        echo "- ghcr.io/${{ github.repository_owner }}/drill-hive-test:standard"
        echo "- ghcr.io/${{ github.repository_owner }}/drill-hive-test:preinitialized"
        echo "- ghcr.io/${{ github.repository_owner }}/drill-hive-test:latest"
```

### Make Image Public

After pushing, make the image public (if desired):

1. Go to your GitHub profile → Packages
2. Click on `drill-hive-test`
3. Click **"Package settings"** (bottom right)
4. Scroll to **"Danger Zone"** → **"Change visibility"**
5. Select **"Public"** and confirm

---

## Using the Image in Tests

### Update GitHub Actions Test Workflow

Modify `.github/workflows/hive-tests.yml`:

```yaml
name: Hive Storage Tests

on:
  push:
    branches: [ master, main ]
  pull_request:
    branches: [ master, main ]

jobs:
  hive-tests:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Set up JDK 11
      uses: actions/setup-java@v3
      with:
        java-version: '11'
        distribution: 'temurin'
        cache: 'maven'

    - name: Log in to GitHub Container Registry
      uses: docker/login-action@v2
      with:
        registry: ghcr.io
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    - name: Pull pre-initialized Hive image
      run: |
        docker pull ghcr.io/${{ github.repository_owner }}/drill-hive-test:preinitialized
        docker tag ghcr.io/${{ github.repository_owner }}/drill-hive-test:preinitialized drill-hive-test:preinitialized

    - name: Build with Maven
      run: mvn clean install -pl contrib/storage-hive/core -am -DskipTests

    - name: Run Hive tests
      run: |
        cd contrib/storage-hive/core
        mvn test -Dhive.image=drill-hive-test:preinitialized
      timeout-minutes: 15
```

### Local Development

Pull and use the image locally:

```bash
# Pull from GHCR
docker pull ghcr.io/apache/drill/hive-test:preinitialized

# Tag for local use
docker tag ghcr.io/apache/drill/hive-test:preinitialized drill-hive-test:preinitialized

# Run tests
cd /Users/charlesgivre/github/drill/contrib/storage-hive/core
mvn test -Dhive.image=drill-hive-test:preinitialized
```

---

## Image Versioning Strategy

### Recommended Tags

Use semantic versioning or date-based tags:

```bash
# Date-based (recommended for data/schema changes)
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:preinitialized-2025-11-12
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:preinitialized-latest

# Semantic versioning (recommended for Hive version changes)
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:3.1.3-preinitialized
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:3.1-preinitialized

# Always maintain 'latest' for default use
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:latest
```

### Update Strategy

```bash
# When Hive version changes (e.g., 3.1.3 → 3.2.0)
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:3.2.0-preinitialized
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:3.2-preinitialized
docker push ghcr.io/apache/drill/hive-test:3.2.0-preinitialized
docker push ghcr.io/apache/drill/hive-test:3.2-preinitialized

# When test data changes (monthly or as needed)
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:preinitialized-$(date +%Y-%m-%d)
docker push ghcr.io/apache/drill/hive-test:preinitialized-$(date +%Y-%m-%d)
```

---

## Cleanup Old Images

### Manual Cleanup

```bash
# List all versions
gh api /users/apache/packages/container/drill-hive-test/versions

# Delete a specific version
gh api --method DELETE /users/apache/packages/container/drill-hive-test/versions/VERSION_ID
```

### Automated Cleanup

GitHub Actions can automatically delete old images:

```yaml
- name: Delete old images
  uses: actions/delete-package-versions@v4
  with:
    package-name: 'drill-hive-test'
    package-type: 'container'
    min-versions-to-keep: 5
    delete-only-untagged-versions: true
```

---

## Troubleshooting

### Issue: Authentication Failed

```bash
# Error: unauthorized: authentication required
```

**Solution:**
```bash
# Re-login with fresh token
docker logout ghcr.io
cat ~/.github-token | docker login ghcr.io -u YOUR_USERNAME --password-stdin
```

### Issue: Permission Denied

```bash
# Error: denied: permission_denied: write_package
```

**Solution:**
- Verify PAT has `write:packages` scope
- Ensure you're the repository owner or have write access
- Check if package already exists with different permissions

### Issue: Image Too Large

```bash
# Error: blob upload unknown
```

**Solution:**
```bash
# Increase Docker daemon storage
docker system prune -a
# Or push in layers
docker push ghcr.io/apache/drill/hive-test:preinitialized --all-tags
```

### Issue: Pull Rate Limit

```bash
# Error: toomanyrequests: You have reached your pull rate limit
```

**Solution:**
- GHCR has generous limits (no rate limit for authenticated pulls from public images)
- Authenticate before pulling:
  ```bash
  docker login ghcr.io -u USERNAME -p TOKEN
  ```

---

## Quick Reference

### Complete Manual Workflow

```bash
# 1. One-time setup
echo "YOUR_TOKEN" > ~/.github-token
chmod 600 ~/.github-token
cat ~/.github-token | docker login ghcr.io -u YOUR_USERNAME --password-stdin

# 2. Build pre-initialized image
cd /Users/charlesgivre/github/drill/contrib/storage-hive/core/src/test/resources/docker
./build-preinitialized-image.sh

# 3. Tag and push
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:preinitialized
docker tag drill-hive-test:preinitialized ghcr.io/apache/drill/hive-test:latest
docker push ghcr.io/apache/drill/hive-test:preinitialized
docker push ghcr.io/apache/drill/hive-test:latest

# 4. Make public (via GitHub UI)
# Visit: https://github.com/apache/drill/packages

# 5. Use in tests
docker pull ghcr.io/apache/drill/hive-test:preinitialized
docker tag ghcr.io/apache/drill/hive-test:preinitialized drill-hive-test:preinitialized
mvn test -Dhive.image=drill-hive-test:preinitialized
```

---

## Summary

**Best Practice for Apache Drill:**

1. ✅ Build pre-initialized image locally or in CI
2. ✅ Push to `ghcr.io/apache/drill/hive-test:preinitialized`
3. ✅ Make image public for all contributors
4. ✅ CI/CD pulls image instead of building (saves 15+ minutes)
5. ✅ Rebuild weekly or when Docker files change

This approach provides the best balance of speed, reliability, and maintainability for the Apache Drill project! 🚀
