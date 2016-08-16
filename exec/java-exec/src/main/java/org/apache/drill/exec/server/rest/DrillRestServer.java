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

import org.apache.drill.exec.ExecConstants;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

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
    final String disableMoxy = PropertiesHelper.getPropertyNameForRuntime(CommonProperties.MOXY_JSON_FEATURE_DISABLE, getConfiguration().getRuntimeType());
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
        } else {
          bindFactory(AnonDrillUserPrincipalProvider.class).to(DrillUserPrincipal.class);
        }
      }
    });
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
    @Inject WorkManager workManager;

    @RequestScoped
    @Override
    public DrillUserPrincipal provide() {
      return new AnonDrillUserPrincipal(workManager.getContext());
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
