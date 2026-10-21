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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Basemap configuration for the geospatial filter builder.
 *
 * <p>The default is no tile server. The map then draws over the vector basemaps already
 * bundled with the web app, so nothing about a deployment's data — not even the area an
 * analyst is looking at — leaves the network. An administrator opts in to raster tiles
 * for street-level detail.
 *
 * <p>Reading is open to any authenticated user because the map needs it to render;
 * writing is admin-only.
 *
 * <p>See {@code docs/dev/GeoFilterBuilder.md}.
 */
@Path("/api/v1/map/config")
@Tag(name = "Map Configuration", description = "Basemap tile configuration")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class MapConfigResources {

  private static final Logger logger = LoggerFactory.getLogger(MapConfigResources.class);

  private static final String CONFIG_STORE_NAME = "drill.sqllab.map_config";
  private static final String CONFIG_KEY = "default";

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<MapConfig> cachedStore;

  /**
   * An XYZ tile template. Without the coordinate placeholders every request would be
   * for the same image, which renders as a uniformly wrong map rather than an error.
   */
  private static final String[] REQUIRED_PLACEHOLDERS = {"{z}", "{x}", "{y}"};

  public static class MapConfig {
    /** XYZ template. Empty means bundled vector basemaps only. */
    @JsonProperty
    public String tileUrl;

    /** Credit line shown on the map. Required whenever tileUrl is set. */
    @JsonProperty
    public String attribution;

    public MapConfig() {
      this.tileUrl = "";
      this.attribution = "";
    }
  }

  public static class MessageResponse {
    @JsonProperty
    public String message;

    public MessageResponse(String message) {
      this.message = message;
    }
  }

  private PersistentStore<MapConfig> getStore() {
    if (cachedStore == null) {
      synchronized (MapConfigResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(), MapConfig.class)
                    .name(CONFIG_STORE_NAME)
                    .build());
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access map config store", e);
          }
        }
      }
    }
    return cachedStore;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get basemap configuration",
      description = "Returns the configured tile server, or empty values meaning the "
          + "bundled vector basemaps are used.")
  public MapConfig getConfig() {
    MapConfig stored = getStore().get(CONFIG_KEY);
    return stored != null ? stored : new MapConfig();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Operation(summary = "Set basemap configuration",
      description = "Sets the tile server. An attribution is required alongside a tile "
          + "URL, because most providers require credit as a condition of use.")
  public Response setConfig(MapConfig config) {
    try {
      String tileUrl = config.tileUrl == null ? "" : config.tileUrl.trim();
      String attribution = config.attribution == null ? "" : config.attribution.trim();

      if (!tileUrl.isEmpty()) {
        for (String placeholder : REQUIRED_PLACEHOLDERS) {
          if (!tileUrl.contains(placeholder)) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new MessageResponse(
                    "Tile URL must be an XYZ template containing " + placeholder
                        + ", for example https://example.com/{z}/{x}/{y}.png"))
                .build();
          }
        }
        if (attribution.isEmpty()) {
          return Response.status(Response.Status.BAD_REQUEST)
              .entity(new MessageResponse(
                  "An attribution is required when a tile URL is set. Most tile "
                      + "providers require credit as a condition of use."))
              .build();
        }
      }

      MapConfig toStore = new MapConfig();
      toStore.tileUrl = tileUrl;
      toStore.attribution = attribution;
      getStore().put(CONFIG_KEY, toStore);

      return Response.ok(toStore).build();
    } catch (Exception e) {
      logger.error("Error saving map config", e);
      throw new DrillRuntimeException("Failed to save map config: " + e.getMessage(), e);
    }
  }
}
