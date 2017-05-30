/**
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
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.WebUserConnection.AnonWebUserConnection;
import org.apache.drill.exec.server.rest.auth.AuthDynamicFeature;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.AnonDrillUserPrincipal;
import org.apache.drill.exec.server.rest.profile.ProfileResources;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Principal;

public class DrillRestServer extends ResourceConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRestServer.class);

  public DrillRestServer(final WorkManager workManager) {
    register(DrillRoot.class);
    register(StatusResources.class);
    register(StorageResources.class);
    register(ProfileResources.class);
    register(QueryResources.class);
    register(MetricsResources.class);
    register(ThreadsResources.class);
    register(LogsResources.class);
    register(FreemarkerMvcFeature.class);
    register(MultiPartFeature.class);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);

    final boolean isAuthEnabled =
        workManager.getContext().getConfig().getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);

    if (isAuthEnabled) {
      register(LogInLogOutResources.class);
      register(AuthDynamicFeature.class);
      register(RolesAllowedDynamicFeature.class);
    }

    //disable moxy so it doesn't conflict with jackson.
    final String disableMoxy = PropertiesHelper.getPropertyNameForRuntime(CommonProperties.MOXY_JSON_FEATURE_DISABLE,
        getConfiguration().getRuntimeType());
    property(disableMoxy, true);

    register(JsonParseExceptionMapper.class);
    register(JsonMappingExceptionMapper.class);
    register(GenericExceptionMapper.class);

    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(workManager.getContext().getLpPersistence().getMapper());
    register(provider);

    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(workManager).to(WorkManager.class);
        bind(workManager.getContext().getLpPersistence().getMapper()).to(ObjectMapper.class);
        bind(workManager.getContext().getStoreProvider()).to(PersistentStoreProvider.class);
        bind(workManager.getContext().getStorage()).to(StoragePluginRegistry.class);
        bind(new UserAuthEnabled(isAuthEnabled)).to(UserAuthEnabled.class);
        if (isAuthEnabled) {
          bindFactory(DrillUserPrincipalProvider.class).to(DrillUserPrincipal.class);
          bindFactory(AuthWebUserConnectionProvider.class).to(WebUserConnection.class);
        } else {
          bindFactory(AnonDrillUserPrincipalProvider.class).to(DrillUserPrincipal.class);
          bindFactory(AnonWebUserConnectionProvider.class).to(WebUserConnection.class);
        }
      }
    });
  }

  public static class AuthWebUserConnectionProvider implements Factory<WebUserConnection> {

    @Inject
    HttpServletRequest request;

    @Inject
    WorkManager workManager;

    @Override
    public WebUserConnection provide() {
      final HttpSession session = request.getSession();
      final Principal sessionUserPrincipal = request.getUserPrincipal();

      // If there is no valid principal this means user is not logged in yet.
      if (sessionUserPrincipal == null) {
        return null;
      }

      // User is logged in, get/set the WebSessionResources attribute
      WebSessionResources webSessionResources =
              (WebSessionResources) session.getAttribute(WebSessionResources.class.getSimpleName());

      if (webSessionResources == null) {
        // User is login in for the first time
        final DrillbitContext drillbitContext = workManager.getContext();
        final DrillConfig config = drillbitContext.getConfig();
        final UserSession drillUserSession = UserSession.Builder.newBuilder()
                .withCredentials(UserBitShared.UserCredentials.newBuilder()
                        .setUserName(sessionUserPrincipal.getName())
                        .build())
                .withOptionManager(drillbitContext.getOptionManager())
                .setSupportComplexTypes(config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
                .build();

        // Only try getting remote address in first login since it's a costly operation.
        SocketAddress remoteAddress = null;
        try {
          // This can be slow as the underlying library will try to resolve the address
          remoteAddress = new InetSocketAddress(InetAddress.getByName(request.getRemoteAddr()), request.getRemotePort());
          session.setAttribute(SocketAddress.class.getSimpleName(), remoteAddress);
        } catch (Exception ex) {
          //no-op
          logger.trace("Failed to get the remote address of the http session request", ex);
        }

        // Create per session BufferAllocator and set it in session
        final String sessionAllocatorName = String.format("WebServer:AuthUserSession:%s", session.getId());
        final BufferAllocator sessionAllocator = workManager.getContext().getAllocator().newChildAllocator(
                sessionAllocatorName,
                config.getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
                config.getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));

        // Create a WebSessionResource instance which owns the lifecycle of all the session resources.
        // Set this instance as an attribute of HttpSession, since it will be used until session is destroyed.
        webSessionResources = new WebSessionResources(sessionAllocator, remoteAddress, drillUserSession);
        session.setAttribute(WebSessionResources.class.getSimpleName(), webSessionResources);
      }
      // Create a new WebUserConnection for the request
      return new WebUserConnection(webSessionResources);
    }

    @Override
    public void dispose(WebUserConnection instance) {

    }
  }

  public static class AnonWebUserConnectionProvider implements Factory<WebUserConnection> {

    @Inject
    HttpServletRequest request;

    @Inject
    WorkManager workManager;

    @Override
    public WebUserConnection provide() {
      final HttpSession session = request.getSession();
      final DrillbitContext drillbitContext = workManager.getContext();
      final DrillConfig config = drillbitContext.getConfig();

      // Create an allocator here for each request
      final BufferAllocator sessionAllocator = drillbitContext.getAllocator()
              .newChildAllocator("WebServer:AnonUserSession",
                      config.getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
                      config.getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));

      final Principal sessionUserPrincipal = new AnonDrillUserPrincipal();

      // Create new UserSession for each request from Anonymous user
      final UserSession drillUserSession = UserSession.Builder.newBuilder()
              .withCredentials(UserBitShared.UserCredentials.newBuilder()
                      .setUserName(sessionUserPrincipal.getName())
                      .build())
              .withOptionManager(drillbitContext.getOptionManager())
              .setSupportComplexTypes(drillbitContext.getConfig().getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
              .build();

      // Try to get the remote Address but set it to null in case of failure.
      SocketAddress remoteAddress = null;
      try {
        // This can be slow as the underlying library will try to resolve the address
        remoteAddress = new InetSocketAddress(InetAddress.getByName(request.getRemoteAddr()), request.getRemotePort());
      } catch (Exception ex) {
        // no-op
        logger.trace("Failed to get the remote address of the http session request", ex);
      }

      final WebSessionResources webSessionResources = new WebSessionResources(sessionAllocator,
              remoteAddress, drillUserSession);

      // Create a AnonWenUserConnection for this request
      return new AnonWebUserConnection(webSessionResources);
    }

    @Override
    public void dispose(WebUserConnection instance) {

    }
  }

  // Provider which injects DrillUserPrincipal directly instead of getting it from SecurityContext and typecasting
  public static class DrillUserPrincipalProvider implements Factory<DrillUserPrincipal> {

    @Inject HttpServletRequest request;

    @Override
    public DrillUserPrincipal provide() {
      return (DrillUserPrincipal) request.getUserPrincipal();
    }

    @Override
    public void dispose(DrillUserPrincipal principal) {
      // No-Op
    }
  }

  // Provider which creates and cleanups DrillUserPrincipal for anonymous (auth disabled) mode
  public static class AnonDrillUserPrincipalProvider implements Factory<DrillUserPrincipal> {

    @RequestScoped
    @Override
    public DrillUserPrincipal provide() {
      return new AnonDrillUserPrincipal();
    }

    @Override
    public void dispose(DrillUserPrincipal principal) {
      // If this worked it would have been clean to free the resources here, but there are various scenarios
      // where dispose never gets called due to bugs in jersey.
    }
  }

  // Returns whether auth is enabled or not in config
  public static class UserAuthEnabled {
    private boolean value;

    public UserAuthEnabled(boolean value) {
      this.value = value;
    }

    public boolean get() {
      return value;
    }
  }
}
