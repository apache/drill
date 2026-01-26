# Jetty 12 Migration Guide

## Overview

Apache Drill has been upgraded from Jetty 9 to Jetty 12 to address security vulnerabilities and maintain compatibility with modern Java versions.

## What Changed

### Core API Changes

1. **Servlet API Migration**: `javax.servlet.*` → `jakarta.servlet.*`
2. **Package Restructuring**: Servlet components moved to `org.eclipse.jetty.ee10.servlet.*`
3. **Handler API Redesign**: New `org.eclipse.jetty.server.Handler` interface
4. **Authentication APIs**: New `LoginService.login()` and authenticator signatures

### Modified Files

#### Key Changes

- **WebServer.java**: Updated resource loading, handler configuration, and security handler setup
- **DrillHttpSecurityHandlerProvider.java**: Refactored from `Handler.Wrapper` to extend `ee10.servlet.security.ConstraintSecurityHandler` for proper session management
- **DrillSpnegoAuthenticator.java**: Updated to Jetty 12 APIs with new `validateRequest(Request, Response, Callback)` signature
- **DrillSpnegoLoginService.java**: Updated `login()` method signature
- **DrillErrorHandler.java**: Migrated to use `generateAcceptableResponse()` for content negotiation
- **YARN WebServer.java**: Updated for Jetty 12 APIs and `IdentityService.newUserIdentity()`

#### Authentication Architecture

The authentication system was redesigned for Jetty 12:

- **DrillHttpSecurityHandlerProvider** now extends `ConstraintSecurityHandler` (previously `Handler.Wrapper`)
- Implements a `RoutingAuthenticator` that delegates to child authenticators (SPNEGO, FORM, BASIC)
- Handles session caching manually since delegated authenticators require explicit session management
- Properly integrated with `ServletContextHandler` via `setSecurityHandler()`

## Known Limitations

### Hadoop MiniDFSCluster Test Incompatibility

**Issue**: Tests using Hadoop's MiniDFSCluster cannot run due to Jetty version conflicts (Hadoop 3.x uses Jetty 9).

**Affected Tests** (temporarily disabled):
- `TestImpersonationDisabledWithMiniDFS.java`
- `TestImpersonationMetadata.java`
- `TestImpersonationQueries.java`
- `TestInboundImpersonation.java`

**Resolution**: Tests will be re-enabled when Apache Hadoop 4.x or a Hadoop 3.x maintenance release upgrades to Jetty 12 (tracked in [HADOOP-19625](https://issues.apache.org/jira/browse/HADOOP-19625)).

## Developer Guidelines

### Writing New Web Server Code

1. Use Jakarta EE 10 imports:
   ```java
   import jakarta.servlet.http.HttpServletRequest;
   import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
   ```

2. Use Jetty constants:
   ```java
   import org.eclipse.jetty.security.Authenticator;
   String authMethod = Authenticator.SPNEGO_AUTH;  // Not "SPNEGO"
   ```

3. For custom error handling, use `generateAcceptableResponse()`:
   ```java
   @Override
   protected void generateAcceptableResponse(ServletContextRequest baseRequest,
                                            HttpServletRequest request,
                                            HttpServletResponse response,
                                            int code, String message,
                                            String contentType) {
     // Use contentType parameter, not request path
   }
   ```

### Writing Authentication Code

When implementing custom authenticators:

1. Extend `LoginAuthenticator` and implement `validateRequest(Request, Response, Callback)`
2. Use `Request.as(request, ServletContextRequest.class)` to access servlet APIs from core Request
3. Return `AuthenticationState` (CHALLENGE, SEND_SUCCESS, or UserAuthenticationSucceeded)
4. Use `Response.writeError()` to properly send challenges with callback completion

Example:
```java
public class CustomAuthenticator extends LoginAuthenticator {
  @Override
  public AuthenticationState validateRequest(Request request, Response response, Callback callback) {
    ServletContextRequest servletRequest = Request.as(request, ServletContextRequest.class);
    // ... authentication logic ...
    if (authFailed) {
      response.getHeaders().put(HttpHeader.WWW_AUTHENTICATE, "Bearer");
      Response.writeError(request, response, callback, HttpStatus.UNAUTHORIZED_401);
      return AuthenticationState.CHALLENGE;
    }
    return new UserAuthenticationSucceeded(getAuthenticationType(), userIdentity);
  }
}
```

### Writing Tests

1. **Use integration tests**: Test with real Drill server and `OkHttpClient`, not mocked servlets
   ```java
   public class MyWebTest extends ClusterTest {
     @Test
     public void testEndpoint() throws Exception {
       String url = String.format("http://localhost:%d/api/endpoint", port);
       Request request = new Request.Builder().url(url).build();
       try (Response response = httpClient.newCall(request).execute()) {
         assertEquals(200, response.code());
       }
     }
   }
   ```

2. **Avoid MiniDFSCluster** in tests that start Drill's HTTP server
3. **Session cookie names**: Tests should accept both "JSESSIONID" and "Drill-Session-Id"

## Dependency Management

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

## Migration Checklist for Future Updates

- [ ] Update Jetty BOM version in parent POM
- [ ] Run full test suite including integration tests
- [ ] Verify checkstyle compliance
- [ ] Check HADOOP-19625 status for MiniDFSCluster test re-enablement
- [ ] Update this document with any new changes

## References

- [Jetty 12 Migration Guide](https://eclipse.dev/jetty/documentation/jetty-12/migration-guide/index.html)
- [Jakarta EE 10 Documentation](https://jakarta.ee/specifications/platform/10/)
- [HADOOP-19625: Upgrade Jetty to 12.x](https://issues.apache.org/jira/browse/HADOOP-19625)
