# Jetty 12 Migration Guide

## Overview

Apache Drill has been upgraded from Jetty 9 to Jetty 12 to address security vulnerabilities and maintain compatibility with modern Java versions. This document describes the changes made, known limitations, and guidance for developers.

## What Changed

### Core API Changes

Jetty 12 introduced significant API changes as part of the Jakarta EE 10 migration:

1. **Servlet API Migration**: `javax.servlet.*` → `jakarta.servlet.*`
2. **Package Restructuring**: Servlet components moved to `org.eclipse.jetty.ee10.servlet.*`
3. **Handler API Redesign**: New `org.eclipse.jetty.server.Handler` interface
4. **Resource Loading**: New `ResourceFactory` API replaces old `Resource` API
5. **Authentication APIs**: `LoginService.login()` signature changed

### Modified Files

#### Production Code

- **exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/WebServer.java**
  - Updated to use `ResourceFactory.root()` for resource loading
  - Fixed null pointer issues when HTTP server is disabled

- **drill-yarn/src/main/java/org/apache/drill/yarn/appMaster/http/WebServer.java**
  - Updated all imports to `org.eclipse.jetty.ee10.servlet.*`
  - Modified `LoginService.login()` to new signature with `Function<Boolean, Session>` parameter
  - Changed to use `IdentityService.newUserIdentity()` for user identity creation
  - Updated `ResourceFactory` API usage
  - Updated `SessionAuthentication` constants
  - Fixed `ServletContextHandler` constructor usage

#### Test Code

The following test classes are temporarily disabled (see Known Limitations below):
- `TestImpersonationDisabledWithMiniDFS.java`
- `TestImpersonationMetadata.java`
- `TestImpersonationQueries.java`
- `TestInboundImpersonation.java`

## Known Limitations

### Hadoop MiniDFSCluster Test Incompatibility

**Issue**: Tests using Hadoop's MiniDFSCluster cannot run due to Jetty version conflicts.

**Root Cause**: Apache Hadoop 3.x depends on Jetty 9, while Drill now uses Jetty 12. When tests attempt to start both:
- Drill's embedded web server (Jetty 12)
- Hadoop's MiniDFSCluster (Jetty 9)

The conflicting Jetty versions on the classpath cause `NoClassDefFoundError` exceptions.

**Affected Tests**:
- Impersonation tests with HDFS
- Any tests requiring MiniDFSCluster with Drill's HTTP server enabled

**Resolution Timeline**: These tests will be re-enabled when:
- Apache Hadoop 4.x is released with Jetty 12 support
- A Hadoop 3.x maintenance release upgrades to Jetty 12 (tracked in [HADOOP-19625](https://issues.apache.org/jira/browse/HADOOP-19625))

**Current Status**: HADOOP-19625 is open and targets Jetty 12 EE10, but requires Java 17 baseline (tracked in HADOOP-17177). No specific release version or timeline is available yet.

### Why Alternative Solutions Failed

Several approaches were attempted to resolve the Jetty conflict:

1. **Dual Jetty versions in test scope**: Failed because Maven cannot have two different versions of the same artifact on the classpath simultaneously, and the Jetty BOM forces all Jetty artifacts to the same version.

2. **Disabling Drill's HTTP server in tests**: Failed because drill-java-exec classes are compiled against Jetty 12, and the bytecode contains hard references to Jetty 12 classes that fail to load even when the HTTP server is disabled.

3. **Separate test module with Jetty 9**: Failed because depending on the drill-java-exec JAR (compiled with Jetty 12) brings Jetty 12 class references into the test classpath.

### Workarounds for Developers

If you need to test HDFS impersonation functionality:

1. **Integration tests**: Use a real Hadoop cluster instead of MiniDFSCluster
2. **Manual testing**: Test in HDFS-enabled environments
3. **Alternative tests**: Use tests with local filesystem instead of MiniDFSCluster (see other impersonation tests that don't require HDFS)

## Developer Guidelines

### Writing New Web Server Code

When adding new HTTP/servlet functionality:

1. Use Jakarta EE 10 imports:
   ```java
   import jakarta.servlet.http.HttpServletRequest;
   import jakarta.servlet.http.HttpServletResponse;
   ```

2. Use Jetty 12 EE10 servlet packages:
   ```java
   import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
   import org.eclipse.jetty.ee10.servlet.ServletHolder;
   ```

3. Use `ResourceFactory.root()` for resource loading:
   ```java
   ResourceFactory rf = ResourceFactory.root();
   InputStream stream = rf.newClassLoaderResource("/path/to/resource").newInputStream();
   ```

4. Use new `ServletContextHandler` constructor pattern:
   ```java
   ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
   handler.setContextPath("/");
   ```

### Writing Tests

1. Tests that start Drill's HTTP server should **not** use Hadoop MiniDFSCluster
2. If HDFS testing is required, use local filesystem or mark test with `@Ignore` and add comprehensive documentation
3. When adding `@Ignore` for Jetty conflicts, reference `TestImpersonationDisabledWithMiniDFS` for standard explanation

### Debugging Jetty Issues

Common issues and solutions:

- **NoClassDefFoundError for Jetty classes**: Check that all Jetty dependencies are Jetty 12, not Jetty 9
- **ClassNotFoundException for javax.servlet**: Should be `jakarta.servlet` with Jetty 12
- **NullPointerException in ResourceFactory**: Use `ResourceFactory.root()` instead of `ResourceFactory.of(server)`
- **Incompatible types in ServletContextHandler**: Use new constructor pattern with `SESSIONS` constant

## Dependency Management

### Maven BOM

Drill's parent POM includes the Jetty 12 BOM:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.eclipse.jetty</groupId>
      <artifactId>jetty-bom</artifactId>
      <version>12.0.16</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

This ensures all Jetty dependencies use version 12.0.16.

### Key Dependencies

Production dependencies include:
- `jetty-server` - Core server functionality
- `jetty-ee10-servlet` - Servlet support
- `jetty-ee10-servlets` - Standard servlet implementations
- `jetty-security` - Security handlers
- `jetty-util` - Utility classes

## Migration Checklist for Future Updates

When upgrading Jetty versions in the future:

- [ ] Check Jetty release notes for API changes
- [ ] Update Jetty BOM version in parent POM
- [ ] Run full test suite including integration tests
- [ ] Check for deprecation warnings in web server code
- [ ] Verify checkstyle compliance
- [ ] Check HADOOP-19625 status to see if MiniDFSCluster tests can be re-enabled
- [ ] Update this document with any new changes or limitations

## References

- [Jetty 12 Migration Guide](https://eclipse.dev/jetty/documentation/jetty-12/migration-guide/index.html)
- [Jakarta EE 10 Documentation](https://jakarta.ee/specifications/platform/10/)
- [HADOOP-19625: Upgrade Jetty to 12.x](https://issues.apache.org/jira/browse/HADOOP-19625)
- [HADOOP-17177: Java 17 Support](https://issues.apache.org/jira/browse/HADOOP-17177)

## Support

For questions or issues related to Jetty 12 migration:
1. Check existing test classes for examples
2. Review this document and referenced Jetty documentation
3. File an issue on the Apache Drill JIRA with component "Web Server"
