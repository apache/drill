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
 * REST API for managing dashboards.
 * Dashboards combine multiple visualizations into interactive drag-and-drop layouts.
 */
@Path("/api/v1/dashboards")
@Tag(name = "Dashboards", description = "APIs for managing interactive dashboards")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class DashboardResources {
  private static final Logger logger = LoggerFactory.getLogger(DashboardResources.class);
  private static final String STORE_NAME = "drill.sqllab.dashboards";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<Dashboard> cachedStore;

  // ==================== Model Classes ====================

  /**
   * A panel within a dashboard, referencing a visualization or content and its layout position.
   */
  public static class DashboardPanel {
    @JsonProperty
    private String id;
    @JsonProperty
    private String type;
    @JsonProperty
    private String visualizationId;
    @JsonProperty
    private String content;
    @JsonProperty
    private Map<String, String> config;
    @JsonProperty
    private String tabId;
    @JsonProperty
    private int x;
    @JsonProperty
    private int y;
    @JsonProperty
    private int width;
    @JsonProperty
    private int height;

    public DashboardPanel() {
    }

    @JsonCreator
    public DashboardPanel(
        @JsonProperty("id") String id,
        @JsonProperty("type") String type,
        @JsonProperty("visualizationId") String visualizationId,
        @JsonProperty("content") String content,
        @JsonProperty("config") Map<String, String> config,
        @JsonProperty("tabId") String tabId,
        @JsonProperty("x") int x,
        @JsonProperty("y") int y,
        @JsonProperty("width") int width,
        @JsonProperty("height") int height) {
      this.id = id;
      this.type = type != null ? type : "visualization";
      this.visualizationId = visualizationId;
      this.content = content;
      this.config = config;
      this.tabId = tabId;
      this.x = x;
      this.y = y;
      this.width = width;
      this.height = height;
    }

    public String getId() {
      return id;
    }

    public String getType() {
      return type;
    }

    public String getVisualizationId() {
      return visualizationId;
    }

    public String getContent() {
      return content;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public String getTabId() {
      return tabId;
    }

    public int getX() {
      return x;
    }

    public int getY() {
      return y;
    }

    public int getWidth() {
      return width;
    }

    public int getHeight() {
      return height;
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setType(String type) {
      this.type = type;
    }

    public void setVisualizationId(String visualizationId) {
      this.visualizationId = visualizationId;
    }

    public void setContent(String content) {
      this.content = content;
    }

    public void setConfig(Map<String, String> config) {
      this.config = config;
    }

    public void setTabId(String tabId) {
      this.tabId = tabId;
    }

    public void setX(int x) {
      this.x = x;
    }

    public void setY(int y) {
      this.y = y;
    }

    public void setWidth(int width) {
      this.width = width;
    }

    public void setHeight(int height) {
      this.height = height;
    }
  }

  /**
   * A tab within a dashboard for organizing panels into groups.
   */
  public static class DashboardTab {
    @JsonProperty
    private String id;
    @JsonProperty
    private String name;
    @JsonProperty
    private int order;

    public DashboardTab() {
    }

    @JsonCreator
    public DashboardTab(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("order") int order) {
      this.id = id;
      this.name = name;
      this.order = order;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public int getOrder() {
      return order;
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setOrder(int order) {
      this.order = order;
    }
  }

  /**
   * Dashboard model for persistence.
   */
  public static class Dashboard {
    @JsonProperty
    private String id;
    @JsonProperty
    private String name;
    @JsonProperty
    private String description;
    @JsonProperty
    private List<DashboardPanel> panels;
    @JsonProperty
    private List<DashboardTab> tabs;
    @JsonProperty
    private String owner;
    @JsonProperty
    private long createdAt;
    @JsonProperty
    private long updatedAt;
    @JsonProperty
    private int refreshInterval;
    @JsonProperty
    private boolean isPublic;

    public Dashboard() {
    }

    @JsonCreator
    public Dashboard(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("panels") List<DashboardPanel> panels,
        @JsonProperty("tabs") List<DashboardTab> tabs,
        @JsonProperty("owner") String owner,
        @JsonProperty("createdAt") long createdAt,
        @JsonProperty("updatedAt") long updatedAt,
        @JsonProperty("refreshInterval") int refreshInterval,
        @JsonProperty("isPublic") boolean isPublic) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.panels = panels;
      this.tabs = tabs;
      this.owner = owner;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.refreshInterval = refreshInterval;
      this.isPublic = isPublic;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public List<DashboardPanel> getPanels() {
      return panels;
    }

    public List<DashboardTab> getTabs() {
      return tabs;
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

    public int getRefreshInterval() {
      return refreshInterval;
    }

    public boolean isPublic() {
      return isPublic;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void setPanels(List<DashboardPanel> panels) {
      this.panels = panels;
    }

    public void setTabs(List<DashboardTab> tabs) {
      this.tabs = tabs;
    }

    public void setUpdatedAt(long updatedAt) {
      this.updatedAt = updatedAt;
    }

    public void setRefreshInterval(int refreshInterval) {
      this.refreshInterval = refreshInterval;
    }

    public void setPublic(boolean isPublic) {
      this.isPublic = isPublic;
    }
  }

  /**
   * Request body for creating a new dashboard.
   */
  public static class CreateDashboardRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public List<DashboardPanel> panels;
    @JsonProperty
    public List<DashboardTab> tabs;
    @JsonProperty
    public int refreshInterval;
    @JsonProperty
    public boolean isPublic;
  }

  /**
   * Request body for updating a dashboard.
   */
  public static class UpdateDashboardRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public List<DashboardPanel> panels;
    @JsonProperty
    public List<DashboardTab> tabs;
    @JsonProperty
    public Integer refreshInterval;
    @JsonProperty
    public Boolean isPublic;
  }

  /**
   * Response containing a list of dashboards.
   */
  public static class DashboardsResponse {
    @JsonProperty
    public List<Dashboard> dashboards;

    public DashboardsResponse(List<Dashboard> dashboards) {
      this.dashboards = dashboards;
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
  @Operation(summary = "List dashboards", description = "Returns all dashboards accessible by the current user")
  public DashboardsResponse listDashboards() {
    logger.debug("Listing dashboards for user: {}", getCurrentUser());

    List<Dashboard> dashboards = new ArrayList<>();
    String currentUser = getCurrentUser();

    try {
      PersistentStore<Dashboard> store = getStore();
      Iterator<Map.Entry<String, Dashboard>> iterator = store.getAll();

      while (iterator.hasNext()) {
        Map.Entry<String, Dashboard> entry = iterator.next();
        Dashboard dashboard = entry.getValue();

        // Return dashboards owned by user or public dashboards
        if (dashboard.getOwner().equals(currentUser) || dashboard.isPublic()) {
          dashboards.add(dashboard);
        }
      }
    } catch (Exception e) {
      logger.error("Error listing dashboards", e);
      throw new DrillRuntimeException("Failed to list dashboards: " + e.getMessage(), e);
    }

    return new DashboardsResponse(dashboards);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create dashboard", description = "Creates a new dashboard")
  public Response createDashboard(CreateDashboardRequest request) {
    logger.debug("Creating dashboard: {}", request.name);

    if (request.name == null || request.name.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("Dashboard name is required"))
          .build();
    }

    String id = UUID.randomUUID().toString();
    long now = Instant.now().toEpochMilli();

    Dashboard dashboard = new Dashboard(
        id,
        request.name.trim(),
        request.description,
        request.panels != null ? request.panels : new ArrayList<>(),
        request.tabs,
        getCurrentUser(),
        now,
        now,
        request.refreshInterval,
        request.isPublic
    );

    try {
      PersistentStore<Dashboard> store = getStore();
      store.put(id, dashboard);
    } catch (Exception e) {
      logger.error("Error creating dashboard", e);
      throw new DrillRuntimeException("Failed to create dashboard: " + e.getMessage(), e);
    }

    return Response.status(Response.Status.CREATED).entity(dashboard).build();
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get dashboard", description = "Returns a dashboard by ID")
  public Response getDashboard(
      @Parameter(description = "Dashboard ID") @PathParam("id") String id) {
    logger.debug("Getting dashboard: {}", id);

    try {
      PersistentStore<Dashboard> store = getStore();
      Dashboard dashboard = store.get(id);

      if (dashboard == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Dashboard not found"))
            .build();
      }

      // Check access permissions
      if (!dashboard.getOwner().equals(getCurrentUser()) && !dashboard.isPublic()) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Access denied"))
            .build();
      }

      return Response.ok(dashboard).build();
    } catch (Exception e) {
      logger.error("Error getting dashboard", e);
      throw new DrillRuntimeException("Failed to get dashboard: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update dashboard", description = "Updates an existing dashboard")
  public Response updateDashboard(
      @Parameter(description = "Dashboard ID") @PathParam("id") String id,
      UpdateDashboardRequest request) {
    logger.debug("Updating dashboard: {}", id);

    try {
      PersistentStore<Dashboard> store = getStore();
      Dashboard dashboard = store.get(id);

      if (dashboard == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Dashboard not found"))
            .build();
      }

      // Only owner can update
      if (!dashboard.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can update this dashboard"))
            .build();
      }

      // Update fields if provided
      if (request.name != null) {
        dashboard.setName(request.name.trim());
      }
      if (request.description != null) {
        dashboard.setDescription(request.description);
      }
      if (request.panels != null) {
        dashboard.setPanels(request.panels);
      }
      if (request.tabs != null) {
        dashboard.setTabs(request.tabs);
      }
      if (request.refreshInterval != null) {
        dashboard.setRefreshInterval(request.refreshInterval);
      }
      if (request.isPublic != null) {
        dashboard.setPublic(request.isPublic);
      }

      dashboard.setUpdatedAt(Instant.now().toEpochMilli());

      store.put(id, dashboard);

      return Response.ok(dashboard).build();
    } catch (Exception e) {
      logger.error("Error updating dashboard", e);
      throw new DrillRuntimeException("Failed to update dashboard: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete dashboard", description = "Deletes a dashboard")
  public Response deleteDashboard(
      @Parameter(description = "Dashboard ID") @PathParam("id") String id) {
    logger.debug("Deleting dashboard: {}", id);

    try {
      PersistentStore<Dashboard> store = getStore();
      Dashboard dashboard = store.get(id);

      if (dashboard == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Dashboard not found"))
            .build();
      }

      // Only owner can delete
      if (!dashboard.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can delete this dashboard"))
            .build();
      }

      store.delete(id);

      return Response.ok(new MessageResponse("Dashboard deleted successfully")).build();
    } catch (Exception e) {
      logger.error("Error deleting dashboard", e);
      throw new DrillRuntimeException("Failed to delete dashboard: " + e.getMessage(), e);
    }
  }

  // ==================== Helper Methods ====================

  private PersistentStore<Dashboard> getStore() {
    if (cachedStore == null) {
      synchronized (DashboardResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    Dashboard.class
                )
                .name(STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access dashboards store", e);
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
