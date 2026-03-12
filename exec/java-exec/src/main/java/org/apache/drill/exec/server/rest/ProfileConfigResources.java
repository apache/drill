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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
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
import jakarta.ws.rs.core.Response;

/**
 * REST resource for managing Data Profiler configuration.
 * Admin-only access for tuning profiling thresholds.
 */
@Path("/api/v1/profile/config")
@Tag(name = "Profile Configuration", description = "Admin configuration for Data Profiler")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class ProfileConfigResources {

  private static final Logger logger = LoggerFactory.getLogger(ProfileConfigResources.class);
  private static final String CONFIG_STORE_NAME = "drill.sqllab.profile_config";
  private static final String CONFIG_KEY = "default";

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<ProfileConfig> cachedStore;

  // ==================== Config Model ====================

  public static class ProfileConfig {
    @JsonProperty
    public int correlationMaxColumns;

    @JsonProperty
    public int profileMaxRows;

    @JsonProperty
    public int profileSampleSize;

    @JsonProperty
    public int histogramBins;

    @JsonProperty
    public int topKValues;

    public ProfileConfig() {
      this.correlationMaxColumns = 20;
      this.profileMaxRows = 50000;
      this.profileSampleSize = 10000;
      this.histogramBins = 20;
      this.topKValues = 10;
    }

    @JsonCreator
    public ProfileConfig(
        @JsonProperty("correlationMaxColumns") Integer correlationMaxColumns,
        @JsonProperty("profileMaxRows") Integer profileMaxRows,
        @JsonProperty("profileSampleSize") Integer profileSampleSize,
        @JsonProperty("histogramBins") Integer histogramBins,
        @JsonProperty("topKValues") Integer topKValues) {
      this.correlationMaxColumns = correlationMaxColumns != null ? correlationMaxColumns : 20;
      this.profileMaxRows = profileMaxRows != null ? profileMaxRows : 50000;
      this.profileSampleSize = profileSampleSize != null ? profileSampleSize : 10000;
      this.histogramBins = histogramBins != null ? histogramBins : 20;
      this.topKValues = topKValues != null ? topKValues : 10;
    }
  }

  // ==================== Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get profile configuration",
      description = "Returns current Data Profiler configuration")
  public Response getConfig() {
    try {
      PersistentStore<ProfileConfig> store = getStore();
      ProfileConfig config = store.get(CONFIG_KEY);
      if (config == null) {
        config = new ProfileConfig();
      }
      return Response.ok(config).build();
    } catch (Exception e) {
      logger.error("Error reading profile config", e);
      throw new DrillRuntimeException("Failed to read profile configuration: " + e.getMessage(), e);
    }
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update profile configuration",
      description = "Updates Data Profiler configuration. Supports partial updates.")
  public Response updateConfig(ProfileConfig request) {
    try {
      PersistentStore<ProfileConfig> store = getStore();
      ProfileConfig existing = store.get(CONFIG_KEY);
      if (existing == null) {
        existing = new ProfileConfig();
      }

      if (request.correlationMaxColumns > 0) {
        existing.correlationMaxColumns = request.correlationMaxColumns;
      }
      if (request.profileMaxRows > 0) {
        existing.profileMaxRows = request.profileMaxRows;
      }
      if (request.profileSampleSize > 0) {
        existing.profileSampleSize = request.profileSampleSize;
      }
      if (request.histogramBins > 0) {
        existing.histogramBins = request.histogramBins;
      }
      if (request.topKValues > 0) {
        existing.topKValues = request.topKValues;
      }

      store.put(CONFIG_KEY, existing);
      return Response.ok(existing).build();
    } catch (Exception e) {
      logger.error("Error updating profile config", e);
      throw new DrillRuntimeException("Failed to update profile configuration: " + e.getMessage(), e);
    }
  }

  // ==================== Helper ====================

  private PersistentStore<ProfileConfig> getStore() {
    if (cachedStore == null) {
      synchronized (ProfileConfigResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    ProfileConfig.class
                )
                .name(CONFIG_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access profile config store", e);
          }
        }
      }
    }
    return cachedStore;
  }
}
