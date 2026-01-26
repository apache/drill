/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest.auth;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.drill.exec.server.rest.header.ResponseHeadersSettingFilter;
import com.google.common.base.Preconditions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.rpc.security.AuthStringUtil;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.WebServerConstants;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.ee10.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.security.Authenticator.Configuration;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class DrillHttpSecurityHandlerProvider extends ConstraintSecurityHandler {
  private static final Logger logger = LoggerFactory.getLogger(DrillHttpSecurityHandlerProvider.class);

  private final Map<String, DrillHttpConstraintSecurityHandler> securityHandlers =
      CaseInsensitiveMap.newHashMapWithExpectedSize(2);

  private final Map<String, String> responseHeaders;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public DrillHttpSecurityHandlerProvider(DrillConfig config, DrillbitContext drillContext)
      throws DrillbitStartupException {

    Preconditions.checkState(config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED));
    this.responseHeaders = ResponseHeadersSettingFilter.retrieveResponseHeaders(config);
    final Set<String> configuredMechanisms = getHttpAuthMechanisms(config);

    final ScanResult scan = drillContext.getClasspathScan();
    final Set factoryImplsRaw = scan.getImplementations(DrillHttpConstraintSecurityHandler.class);
    logger.debug("Found DrillHttpConstraintSecurityHandler implementations: {}", factoryImplsRaw);

    for (final Object obj : factoryImplsRaw) {
      final Class<? extends DrillHttpConstraintSecurityHandler> clazz =
          (Class<? extends DrillHttpConstraintSecurityHandler>) obj;

      // If all the configured mechanisms handler is added then break out of this loop
      if (configuredMechanisms.isEmpty()) {
        break;
      }

      Constructor<? extends DrillHttpConstraintSecurityHandler> validConstructor = null;
      for (final Constructor<?> c : clazz.getConstructors()) {
        final Class<?>[] params = c.getParameterTypes();
        if (params.length == 0) {
          validConstructor = (Constructor<? extends DrillHttpConstraintSecurityHandler>) c; // unchecked
          break;
        }
      }

      if (validConstructor == null) {
        logger.warn("Skipping DrillHttpConstraintSecurityHandler class {}. It must implement at least one" +
            " constructor with signature [{}()]", clazz.getCanonicalName(), clazz.getName());
        continue;
      }

      try {
        final DrillHttpConstraintSecurityHandler instance = validConstructor.newInstance();
        if (configuredMechanisms.remove(instance.getImplName())) {
          instance.doSetup(drillContext);
          securityHandlers.put(instance.getImplName(), instance);
        }
      } catch (IllegalArgumentException | ReflectiveOperationException | DrillException e) {
        logger.warn(String.format("Failed to create DrillHttpConstraintSecurityHandler of type '%s'",
            clazz.getCanonicalName()), e);
      }
    }

    if (securityHandlers.size() == 0) {
      throw new DrillbitStartupException(
          "Authentication is enabled for WebServer but none of the security mechanism " +
          "was configured properly. Please verify the configurations and try again.");
    }

    // Configure this security handler with the routing authenticator
    setAuthenticator(new RoutingAuthenticator());

    // Use the login service from one of the child handlers (they should all use the same one for a given auth method)
    // For SPNEGO or FORM, get the first available login service
    for (DrillHttpConstraintSecurityHandler handler : securityHandlers.values()) {
      if (handler.getLoginService() != null) {
        setLoginService(handler.getLoginService());
        break;
      }
    }

    // Set up constraint mappings to require authentication for all paths
    org.eclipse.jetty.security.Constraint constraint = new org.eclipse.jetty.security.Constraint.Builder()
        .name("AUTH")
        .roles(DrillUserPrincipal.AUTHENTICATED_ROLE)
        .build();

    org.eclipse.jetty.ee10.servlet.security.ConstraintMapping mapping = new org.eclipse.jetty.ee10.servlet.security.ConstraintMapping();
    mapping.setPathSpec("/*");
    mapping.setConstraint(constraint);

    setConstraintMappings(java.util.Collections.singletonList(mapping),
        com.google.common.collect.ImmutableSet.of(DrillUserPrincipal.AUTHENTICATED_ROLE, DrillUserPrincipal.ADMIN_ROLE));

    // Enable session management for authentication caching
    setSessionRenewedOnAuthentication(true);

    logger.info("Configure auth mechanisms for WebServer are: {}", securityHandlers.keySet());
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
    for (DrillHttpConstraintSecurityHandler securityHandler : securityHandlers.values()) {
      securityHandler.doStart();
    }
  }

  /**
   * Custom authenticator that routes to the appropriate child authenticator
   * based on the request URI and authentication type.
   */
  private class RoutingAuthenticator implements Authenticator {
    @Override
    public String getAuthenticationType() {
      return "ROUTING";
    }

    @Override
    public void setConfiguration(Configuration configuration) {
      // No-op - configuration is handled by child authenticators
    }

    @Override
    public AuthenticationState validateRequest(Request request, Response response, Callback callback) throws ServerAuthException {
      try {
        // Get servlet request for routing decisions
        ServletContextRequest servletContextRequest = Request.as(request, ServletContextRequest.class);
        if (servletContextRequest == null) {
          return AuthenticationState.SEND_SUCCESS;
        }

        HttpServletRequest httpReq = servletContextRequest.getServletApiRequest();
        String uri = httpReq.getRequestURI();
        String authHeader = httpReq.getHeader(HttpHeader.AUTHORIZATION.asString());

        logger.debug("Routing authentication for URI: {}", uri);

        // Check for existing authentication in session first
        try {
          jakarta.servlet.http.HttpSession session = httpReq.getSession(false);
          if (session != null) {
            org.eclipse.jetty.security.authentication.SessionAuthentication sessionAuth =
                (org.eclipse.jetty.security.authentication.SessionAuthentication)
                    session.getAttribute(org.eclipse.jetty.security.authentication.SessionAuthentication.AUTHENTICATED_ATTRIBUTE);
            if (sessionAuth != null) {
              logger.debug("Using cached authentication for: {}", sessionAuth.getUserIdentity().getUserPrincipal().getName());
              return sessionAuth;
            }
          }
        } catch (Exception e) {
          logger.debug("Could not check session for existing authentication", e);
        }

        final DrillHttpConstraintSecurityHandler securityHandler;

        // Route to the appropriate security handler based on URI and configuration
        // SPNEGO authentication for /spnegoLogin path
        if (isSpnegoEnabled() && uri.endsWith(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH)) {
          securityHandler = securityHandlers.get(Authenticator.SPNEGO_AUTH);
        }
        // Basic authentication if Authorization header is present
        else if (isBasicEnabled() && authHeader != null) {
          securityHandler = securityHandlers.get(Authenticator.BASIC_AUTH);
        }
        // Form authentication for all other paths (if enabled)
        else if (isFormEnabled()) {
          securityHandler = securityHandlers.get(Authenticator.FORM_AUTH);
        }
        // SPNEGO-only mode - route all requests through SPNEGO
        else if (isSpnegoEnabled()) {
          securityHandler = securityHandlers.get(Authenticator.SPNEGO_AUTH);
        }
        else {
          logger.debug("No authenticator matched for URI: {}", uri);
          return AuthenticationState.SEND_SUCCESS;
        }

        // Get the authenticator from the selected security handler and delegate to it
        Authenticator authenticator = securityHandler.getAuthenticator();
        if (authenticator != null) {
          AuthenticationState authState = authenticator.validateRequest(request, response, callback);

          // If authentication succeeded, manually cache it in the session
          // (Jetty's ConstraintSecurityHandler doesn't auto-cache when using delegated authenticators)
          if (authState instanceof org.eclipse.jetty.security.authentication.LoginAuthenticator.UserAuthenticationSucceeded) {
            try {
              jakarta.servlet.http.HttpSession session = httpReq.getSession(true);
              if (session != null) {
                org.eclipse.jetty.security.UserIdentity userIdentity =
                    ((org.eclipse.jetty.security.authentication.LoginAuthenticator.UserAuthenticationSucceeded) authState).getUserIdentity();
                org.eclipse.jetty.security.authentication.SessionAuthentication sessionAuth =
                    new org.eclipse.jetty.security.authentication.SessionAuthentication(
                        authenticator.getAuthenticationType(), userIdentity, null);
                session.setAttribute(org.eclipse.jetty.security.authentication.SessionAuthentication.AUTHENTICATED_ATTRIBUTE, sessionAuth);
                logger.debug("Cached authentication in session for: {}", userIdentity.getUserPrincipal().getName());
              }
            } catch (Exception e) {
              logger.warn("Could not cache authentication in session", e);
            }
          }

          return authState;
        }

        return AuthenticationState.SEND_SUCCESS;
      } catch (Exception e) {
        logger.error("EXCEPTION in RoutingAuthenticator: " + e.getClass().getName() + ": " + e.getMessage(), e);
        throw new ServerAuthException(e);
      }
    }
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    for (DrillHttpConstraintSecurityHandler securityHandler : securityHandlers.values()) {
      securityHandler.doStop();
    }
  }

  public boolean isSpnegoEnabled() {
    return securityHandlers.containsKey(Authenticator.SPNEGO_AUTH);
  }

  public boolean isFormEnabled() {
    return securityHandlers.containsKey(Authenticator.FORM_AUTH);
  }

  public boolean isBasicEnabled() {
    return securityHandlers.containsKey(Authenticator.BASIC_AUTH);
  }

  /**
   * Returns a list of configured mechanisms for HTTP authentication. For backward
   * compatibility if authentication is enabled it will include FORM mechanism by default.
   * @param config - {@link DrillConfig}
   * @return
   */
  public static Set<String> getHttpAuthMechanisms(DrillConfig config) {
    final Set<String> configuredMechs = new HashSet<>();
    final boolean authEnabled = config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);

    if (authEnabled) {
      if (config.hasPath(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS)) {
        configuredMechs.addAll(
            AuthStringUtil.asSet(config.getStringList(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS)));
      } else {
        // For backward compatibility
        configuredMechs.add(Authenticator.FORM_AUTH);
      }
    }
    return configuredMechs;
  }
}
