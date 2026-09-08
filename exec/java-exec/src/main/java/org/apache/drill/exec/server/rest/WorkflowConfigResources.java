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
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * REST endpoint for workflow (scheduled query) configuration.
 * Stores settings server-side using PersistentStore so they
 * apply to all users.
 */
@Path("/api/v1/workflows")
@Tag(name = "Workflows", description = "Scheduled query workflow configuration")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class WorkflowConfigResources {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowConfigResources.class);
  private static final String STORE_NAME = "drill.sqllab.workflow_config";
  private static final String CONFIG_KEY = "default";

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<WorkflowConfig> cachedStore;

  /**
   * Workflow configuration model stored in PersistentStore.
   */
  public static class WorkflowConfig {
    @JsonProperty
    public boolean expirationEnabled = true;

    @JsonProperty
    public int expirationDays = 90;

    @JsonProperty
    public int warningDaysBeforeExpiry = 14;

    public WorkflowConfig() {
    }

    public WorkflowConfig(boolean expirationEnabled, int expirationDays, int warningDaysBeforeExpiry) {
      this.expirationEnabled = expirationEnabled;
      this.expirationDays = expirationDays;
      this.warningDaysBeforeExpiry = warningDaysBeforeExpiry;
    }
  }

  @GET
  @Path("/config")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get workflow configuration",
      description = "Returns the server-side workflow expiration settings")
  public WorkflowConfig getConfig() {
    WorkflowConfig config = loadConfig();
    if (config == null) {
      return new WorkflowConfig();
    }
    return config;
  }

  @PUT
  @Path("/config")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Operation(summary = "Update workflow configuration",
      description = "Updates the server-side workflow expiration settings (admin only)")
  public WorkflowConfig updateConfig(WorkflowConfig config) {
    try {
      PersistentStore<WorkflowConfig> store = getStore();
      store.put(CONFIG_KEY, config);
      logger.info("Workflow config updated: expirationEnabled={}, expirationDays={}, warningDays={}",
          config.expirationEnabled, config.expirationDays, config.warningDaysBeforeExpiry);
      return config;
    } catch (Exception e) {
      logger.error("Failed to save workflow config", e);
      throw new DrillRuntimeException("Failed to save workflow configuration: " + e.getMessage(), e);
    }
  }

  private WorkflowConfig loadConfig() {
    try {
      PersistentStore<WorkflowConfig> store = getStore();
      return store.get(CONFIG_KEY);
    } catch (Exception e) {
      logger.error("Error reading workflow config", e);
      return null;
    }
  }

  private PersistentStore<WorkflowConfig> getStore() {
    if (cachedStore == null) {
      synchronized (WorkflowConfigResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    WorkflowConfig.class
                )
                .name(STORE_NAME)
                .build()
            );
          } catch (Exception e) {
            throw new DrillRuntimeException("Failed to access workflow config store", e);
          }
        }
      }
    }
    return cachedStore;
  }
}
