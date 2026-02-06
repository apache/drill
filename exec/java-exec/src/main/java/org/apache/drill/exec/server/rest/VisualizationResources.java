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
import io.swagger.v3.oas.annotations.Parameter;
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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for managing visualizations.
 * Visualizations are charts created from saved query results.
 */
@Path("/api/v1/visualizations")
@Tag(name = "Visualizations", description = "APIs for managing chart visualizations")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class VisualizationResources {
  private static final Logger logger = LoggerFactory.getLogger(VisualizationResources.class);
  private static final String STORE_NAME = "drill.sqllab.visualizations";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<Visualization> cachedStore;

  // ==================== Model Classes ====================

  /**
   * Configuration for visualization axes and metrics.
   */
  public static class VisualizationConfig {
    @JsonProperty
    private String xAxis;
    @JsonProperty
    private String yAxis;
    @JsonProperty
    private List<String> metrics;
    @JsonProperty
    private List<String> dimensions;
    @JsonProperty
    private Map<String, Object> chartOptions;
    @JsonProperty
    private String colorScheme;

    public VisualizationConfig() {
    }

    @JsonCreator
    public VisualizationConfig(
        @JsonProperty("xAxis") String xAxis,
        @JsonProperty("yAxis") String yAxis,
        @JsonProperty("metrics") List<String> metrics,
        @JsonProperty("dimensions") List<String> dimensions,
        @JsonProperty("chartOptions") Map<String, Object> chartOptions,
        @JsonProperty("colorScheme") String colorScheme) {
      this.xAxis = xAxis;
      this.yAxis = yAxis;
      this.metrics = metrics;
      this.dimensions = dimensions;
      this.chartOptions = chartOptions;
      this.colorScheme = colorScheme;
    }

    public String getXAxis() {
      return xAxis;
    }

    public String getYAxis() {
      return yAxis;
    }

    public List<String> getMetrics() {
      return metrics;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public Map<String, Object> getChartOptions() {
      return chartOptions;
    }

    public String getColorScheme() {
      return colorScheme;
    }

    public void setXAxis(String xAxis) {
      this.xAxis = xAxis;
    }

    public void setYAxis(String yAxis) {
      this.yAxis = yAxis;
    }

    public void setMetrics(List<String> metrics) {
      this.metrics = metrics;
    }

    public void setDimensions(List<String> dimensions) {
      this.dimensions = dimensions;
    }

    public void setChartOptions(Map<String, Object> chartOptions) {
      this.chartOptions = chartOptions;
    }

    public void setColorScheme(String colorScheme) {
      this.colorScheme = colorScheme;
    }
  }

  /**
   * Visualization model for persistence.
   */
  public static class Visualization {
    @JsonProperty
    private String id;
    @JsonProperty
    private String name;
    @JsonProperty
    private String description;
    @JsonProperty
    private String savedQueryId;
    @JsonProperty
    private String chartType;
    @JsonProperty
    private VisualizationConfig config;
    @JsonProperty
    private String owner;
    @JsonProperty
    private long createdAt;
    @JsonProperty
    private long updatedAt;
    @JsonProperty
    private boolean isPublic;
    @JsonProperty
    private String sql;
    @JsonProperty
    private String defaultSchema;

    public Visualization() {
    }

    @JsonCreator
    public Visualization(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("savedQueryId") String savedQueryId,
        @JsonProperty("chartType") String chartType,
        @JsonProperty("config") VisualizationConfig config,
        @JsonProperty("owner") String owner,
        @JsonProperty("createdAt") long createdAt,
        @JsonProperty("updatedAt") long updatedAt,
        @JsonProperty("isPublic") boolean isPublic,
        @JsonProperty("sql") String sql,
        @JsonProperty("defaultSchema") String defaultSchema) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.savedQueryId = savedQueryId;
      this.chartType = chartType;
      this.config = config;
      this.owner = owner;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.isPublic = isPublic;
      this.sql = sql;
      this.defaultSchema = defaultSchema;
    }

    // Getters
    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public String getSavedQueryId() {
      return savedQueryId;
    }

    public String getChartType() {
      return chartType;
    }

    public VisualizationConfig getConfig() {
      return config;
    }

    public String getOwner() {
      return owner;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public long getUpdatedAt() {
      return updatedAt;
    }

    public boolean isPublic() {
      return isPublic;
    }

    public String getSql() {
      return sql;
    }

    public String getDefaultSchema() {
      return defaultSchema;
    }

    // Setters for updates
    public void setName(String name) {
      this.name = name;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void setSavedQueryId(String savedQueryId) {
      this.savedQueryId = savedQueryId;
    }

    public void setChartType(String chartType) {
      this.chartType = chartType;
    }

    public void setConfig(VisualizationConfig config) {
      this.config = config;
    }

    public void setUpdatedAt(long updatedAt) {
      this.updatedAt = updatedAt;
    }

    public void setPublic(boolean isPublic) {
      this.isPublic = isPublic;
    }

    public void setSql(String sql) {
      this.sql = sql;
    }

    public void setDefaultSchema(String defaultSchema) {
      this.defaultSchema = defaultSchema;
    }
  }

  /**
   * Request body for creating a new visualization.
   */
  public static class CreateVisualizationRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public String savedQueryId;
    @JsonProperty
    public String chartType;
    @JsonProperty
    public VisualizationConfig config;
    @JsonProperty
    public boolean isPublic;
    @JsonProperty
    public String sql;
    @JsonProperty
    public String defaultSchema;
  }

  /**
   * Request body for updating a visualization.
   */
  public static class UpdateVisualizationRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public String savedQueryId;
    @JsonProperty
    public String chartType;
    @JsonProperty
    public VisualizationConfig config;
    @JsonProperty
    public Boolean isPublic;
    @JsonProperty
    public String sql;
    @JsonProperty
    public String defaultSchema;
  }

  /**
   * Response containing a list of visualizations.
   */
  public static class VisualizationsResponse {
    @JsonProperty
    public List<Visualization> visualizations;

    public VisualizationsResponse(List<Visualization> visualizations) {
      this.visualizations = visualizations;
    }
  }

  /**
   * Simple message response.
   */
  public static class MessageResponse {
    @JsonProperty
    public String message;

    public MessageResponse(String message) {
      this.message = message;
    }
  }

  // ==================== API Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List visualizations", description = "Returns all visualizations accessible by the current user")
  public VisualizationsResponse listVisualizations() {
    logger.debug("Listing visualizations for user: {}", getCurrentUser());

    List<Visualization> visualizations = new ArrayList<>();
    String currentUser = getCurrentUser();

    try {
      PersistentStore<Visualization> store = getStore();
      Iterator<Map.Entry<String, Visualization>> iterator = store.getAll();

      while (iterator.hasNext()) {
        Map.Entry<String, Visualization> entry = iterator.next();
        Visualization viz = entry.getValue();

        // Return visualizations owned by user or public visualizations
        if (viz.getOwner().equals(currentUser) || viz.isPublic()) {
          visualizations.add(viz);
        }
      }
    } catch (Exception e) {
      logger.error("Error listing visualizations", e);
      throw new DrillRuntimeException("Failed to list visualizations: " + e.getMessage(), e);
    }

    return new VisualizationsResponse(visualizations);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create visualization", description = "Creates a new visualization")
  public Response createVisualization(CreateVisualizationRequest request) {
    logger.debug("Creating visualization: {}", request.name);

    if (request.name == null || request.name.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("Visualization name is required"))
          .build();
    }

    if (request.chartType == null || request.chartType.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("Chart type is required"))
          .build();
    }

    String id = UUID.randomUUID().toString();
    long now = Instant.now().toEpochMilli();

    Visualization viz = new Visualization(
        id,
        request.name.trim(),
        request.description,
        request.savedQueryId,
        request.chartType,
        request.config != null ? request.config : new VisualizationConfig(),
        getCurrentUser(),
        now,
        now,
        request.isPublic,
        request.sql,
        request.defaultSchema
    );

    try {
      PersistentStore<Visualization> store = getStore();
      store.put(id, viz);
    } catch (Exception e) {
      logger.error("Error creating visualization", e);
      throw new DrillRuntimeException("Failed to create visualization: " + e.getMessage(), e);
    }

    return Response.status(Response.Status.CREATED).entity(viz).build();
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get visualization", description = "Returns a visualization by ID")
  public Response getVisualization(
      @Parameter(description = "Visualization ID") @PathParam("id") String id) {
    logger.debug("Getting visualization: {}", id);

    try {
      PersistentStore<Visualization> store = getStore();
      Visualization viz = store.get(id);

      if (viz == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Visualization not found"))
            .build();
      }

      // Check access permissions
      if (!viz.getOwner().equals(getCurrentUser()) && !viz.isPublic()) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Access denied"))
            .build();
      }

      return Response.ok(viz).build();
    } catch (Exception e) {
      logger.error("Error getting visualization", e);
      throw new DrillRuntimeException("Failed to get visualization: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update visualization", description = "Updates an existing visualization")
  public Response updateVisualization(
      @Parameter(description = "Visualization ID") @PathParam("id") String id,
      UpdateVisualizationRequest request) {
    logger.debug("Updating visualization: {}", id);

    try {
      PersistentStore<Visualization> store = getStore();
      Visualization viz = store.get(id);

      if (viz == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Visualization not found"))
            .build();
      }

      // Only owner can update
      if (!viz.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can update this visualization"))
            .build();
      }

      // Update fields if provided
      if (request.name != null) {
        viz.setName(request.name.trim());
      }
      if (request.description != null) {
        viz.setDescription(request.description);
      }
      if (request.savedQueryId != null) {
        viz.setSavedQueryId(request.savedQueryId);
      }
      if (request.chartType != null) {
        viz.setChartType(request.chartType);
      }
      if (request.config != null) {
        viz.setConfig(request.config);
      }
      if (request.isPublic != null) {
        viz.setPublic(request.isPublic);
      }
      if (request.sql != null) {
        viz.setSql(request.sql);
      }
      if (request.defaultSchema != null) {
        viz.setDefaultSchema(request.defaultSchema);
      }

      viz.setUpdatedAt(Instant.now().toEpochMilli());

      store.put(id, viz);

      return Response.ok(viz).build();
    } catch (Exception e) {
      logger.error("Error updating visualization", e);
      throw new DrillRuntimeException("Failed to update visualization: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete visualization", description = "Deletes a visualization")
  public Response deleteVisualization(
      @Parameter(description = "Visualization ID") @PathParam("id") String id) {
    logger.debug("Deleting visualization: {}", id);

    try {
      PersistentStore<Visualization> store = getStore();
      Visualization viz = store.get(id);

      if (viz == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Visualization not found"))
            .build();
      }

      // Only owner can delete
      if (!viz.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can delete this visualization"))
            .build();
      }

      store.delete(id);

      return Response.ok(new MessageResponse("Visualization deleted successfully")).build();
    } catch (Exception e) {
      logger.error("Error deleting visualization", e);
      throw new DrillRuntimeException("Failed to delete visualization: " + e.getMessage(), e);
    }
  }

  // ==================== Helper Methods ====================

  private PersistentStore<Visualization> getStore() {
    if (cachedStore == null) {
      synchronized (VisualizationResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    Visualization.class
                )
                .name(STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access visualizations store", e);
          }
        }
      }
    }
    return cachedStore;
  }

  private String getCurrentUser() {
    return principal.getName();
  }
}
