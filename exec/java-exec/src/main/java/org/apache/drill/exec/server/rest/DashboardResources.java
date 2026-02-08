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
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final String FAVORITES_STORE_NAME = "drill.sqllab.dashboard_favorites";
  private static final String UPLOAD_DIR_NAME = "dashboard-images";
  private static final long MAX_FILE_SIZE = 5 * 1024 * 1024; // 5 MB
  private static final Set<String> ALLOWED_EXTENSIONS = new HashSet<>(
      Arrays.asList("jpg", "jpeg", "png", "gif", "svg", "webp"));

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<Dashboard> cachedStore;
  private static volatile PersistentStore<UserFavorites> cachedFavoritesStore;
  private static volatile File cachedUploadDir;

  // ==================== Model Classes ====================

  /**
   * Theme configuration for a dashboard's visual appearance.
   */
  public static class DashboardTheme {
    @JsonProperty
    private String mode;
    @JsonProperty
    private String fontFamily;
    @JsonProperty
    private String backgroundColor;
    @JsonProperty
    private String fontColor;
    @JsonProperty
    private String panelBackground;
    @JsonProperty
    private String panelBorderColor;
    @JsonProperty
    private String panelBorderRadius;
    @JsonProperty
    private String accentColor;
    @JsonProperty
    private String headerColor;

    public DashboardTheme() {
    }

    @JsonCreator
    public DashboardTheme(
        @JsonProperty("mode") String mode,
        @JsonProperty("fontFamily") String fontFamily,
        @JsonProperty("backgroundColor") String backgroundColor,
        @JsonProperty("fontColor") String fontColor,
        @JsonProperty("panelBackground") String panelBackground,
        @JsonProperty("panelBorderColor") String panelBorderColor,
        @JsonProperty("panelBorderRadius") String panelBorderRadius,
        @JsonProperty("accentColor") String accentColor,
        @JsonProperty("headerColor") String headerColor) {
      this.mode = mode;
      this.fontFamily = fontFamily;
      this.backgroundColor = backgroundColor;
      this.fontColor = fontColor;
      this.panelBackground = panelBackground;
      this.panelBorderColor = panelBorderColor;
      this.panelBorderRadius = panelBorderRadius;
      this.accentColor = accentColor;
      this.headerColor = headerColor;
    }

    public String getMode() {
      return mode;
    }

    public String getFontFamily() {
      return fontFamily;
    }

    public String getBackgroundColor() {
      return backgroundColor;
    }

    public String getFontColor() {
      return fontColor;
    }

    public String getPanelBackground() {
      return panelBackground;
    }

    public String getPanelBorderColor() {
      return panelBorderColor;
    }

    public String getPanelBorderRadius() {
      return panelBorderRadius;
    }

    public String getAccentColor() {
      return accentColor;
    }

    public String getHeaderColor() {
      return headerColor;
    }

    public void setMode(String mode) {
      this.mode = mode;
    }

    public void setFontFamily(String fontFamily) {
      this.fontFamily = fontFamily;
    }

    public void setBackgroundColor(String backgroundColor) {
      this.backgroundColor = backgroundColor;
    }

    public void setFontColor(String fontColor) {
      this.fontColor = fontColor;
    }

    public void setPanelBackground(String panelBackground) {
      this.panelBackground = panelBackground;
    }

    public void setPanelBorderColor(String panelBorderColor) {
      this.panelBorderColor = panelBorderColor;
    }

    public void setPanelBorderRadius(String panelBorderRadius) {
      this.panelBorderRadius = panelBorderRadius;
    }

    public void setAccentColor(String accentColor) {
      this.accentColor = accentColor;
    }

    public void setHeaderColor(String headerColor) {
      this.headerColor = headerColor;
    }
  }

  /**
   * Stores a user's list of favorited dashboard IDs.
   */
  public static class UserFavorites {
    @JsonProperty
    private List<String> dashboardIds;

    public UserFavorites() {
      this.dashboardIds = new ArrayList<>();
    }

    @JsonCreator
    public UserFavorites(
        @JsonProperty("dashboardIds") List<String> dashboardIds) {
      this.dashboardIds = dashboardIds != null ? dashboardIds : new ArrayList<>();
    }

    public List<String> getDashboardIds() {
      return dashboardIds;
    }

    public void setDashboardIds(List<String> dashboardIds) {
      this.dashboardIds = dashboardIds;
    }
  }

  /**
   * Response for favorite toggle operations.
   */
  public static class FavoriteResponse {
    @JsonProperty
    public boolean favorited;
    @JsonProperty
    public String message;

    public FavoriteResponse(boolean favorited, String message) {
      this.favorited = favorited;
      this.message = message;
    }
  }

  /**
   * Response containing a list of favorited dashboard IDs.
   */
  public static class FavoritesListResponse {
    @JsonProperty
    public List<String> dashboardIds;

    public FavoritesListResponse(List<String> dashboardIds) {
      this.dashboardIds = dashboardIds;
    }
  }

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
    private DashboardTheme theme;
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
        @JsonProperty("theme") DashboardTheme theme,
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
      this.theme = theme;
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

    public DashboardTheme getTheme() {
      return theme;
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

    public void setTheme(DashboardTheme theme) {
      this.theme = theme;
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
    public DashboardTheme theme;
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
    public DashboardTheme theme;
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

  /**
   * Response for image upload operations.
   */
  public static class ImageUploadResponse {
    @JsonProperty
    public String url;
    @JsonProperty
    public String filename;

    public ImageUploadResponse(String url, String filename) {
      this.url = url;
      this.filename = filename;
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
        request.theme,
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
      if (request.theme != null) {
        dashboard.setTheme(request.theme);
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

  // ==================== Image Upload Endpoints ====================

  @POST
  @Path("/upload-image")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Upload image", description = "Uploads an image file for use in dashboard panels")
  public Response uploadImage(
      @FormDataParam("file") InputStream fileInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail) {

    if (fileInputStream == null || fileDetail == null || fileDetail.getFileName() == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("No file provided"))
          .build();
    }

    String originalFilename = fileDetail.getFileName();
    String ext = getFileExtension(originalFilename);

    if (!ALLOWED_EXTENSIONS.contains(ext)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse(
              "Invalid file type. Allowed: " + String.join(", ", ALLOWED_EXTENSIONS)))
          .build();
    }

    String storedFilename = UUID.randomUUID() + "." + ext;
    File uploadDir = getUploadDir();
    File targetFile = new File(uploadDir, storedFilename);

    try {
      long totalBytes = 0;
      byte[] buffer = new byte[8192];
      int bytesRead;

      try (FileOutputStream fos = new FileOutputStream(targetFile)) {
        while ((bytesRead = fileInputStream.read(buffer)) != -1) {
          totalBytes += bytesRead;
          if (totalBytes > MAX_FILE_SIZE) {
            fos.close();
            if (!targetFile.delete()) {
              logger.warn("Failed to delete oversized upload: {}", targetFile);
            }
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new MessageResponse("File exceeds maximum size of 5 MB"))
                .build();
          }
          fos.write(buffer, 0, bytesRead);
        }
      }
    } catch (IOException e) {
      if (targetFile.exists() && !targetFile.delete()) {
        logger.warn("Failed to clean up partial upload: {}", targetFile);
      }
      logger.error("Error uploading image", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new MessageResponse("Failed to upload image: " + e.getMessage()))
          .build();
    }

    String url = "/api/v1/dashboards/images/" + storedFilename;
    return Response.status(Response.Status.CREATED)
        .entity(new ImageUploadResponse(url, originalFilename))
        .build();
  }

  @GET
  @Path("/images/{filename}")
  @Operation(summary = "Get uploaded image", description = "Serves a previously uploaded dashboard image")
  public Response getImage(
      @Parameter(description = "Image filename")
      @PathParam("filename") String filename) {

    // Strict validation: UUID + allowed extension only
    if (!filename.matches("[a-f0-9\\-]+\\.(jpg|jpeg|png|gif|svg|webp)")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("Invalid filename"))
          .build();
    }

    File uploadDir = getUploadDir();
    File imageFile = new File(uploadDir, filename);

    if (!imageFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(new MessageResponse("Image not found"))
          .build();
    }

    String ext = getFileExtension(filename);
    String contentType;
    switch (ext) {
      case "jpg":
      case "jpeg":
        contentType = "image/jpeg";
        break;
      case "png":
        contentType = "image/png";
        break;
      case "gif":
        contentType = "image/gif";
        break;
      case "svg":
        contentType = "image/svg+xml";
        break;
      case "webp":
        contentType = "image/webp";
        break;
      default:
        contentType = "application/octet-stream";
        break;
    }

    return Response.ok(imageFile, contentType)
        .header("Cache-Control", "public, max-age=86400")
        .header("Content-Disposition", "inline")
        .header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
        .header("X-Content-Type-Options", "nosniff")
        .build();
  }

  // ==================== Favorites Endpoints ====================

  @GET
  @Path("/favorites")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get favorites", description = "Returns the current user's favorited dashboard IDs")
  public FavoritesListResponse getFavorites() {
    logger.debug("Getting favorites for user: {}", getCurrentUser());

    try {
      PersistentStore<UserFavorites> store = getFavoritesStore();
      UserFavorites favorites = store.get(getCurrentUser());

      if (favorites == null) {
        return new FavoritesListResponse(new ArrayList<>());
      }

      return new FavoritesListResponse(favorites.getDashboardIds());
    } catch (Exception e) {
      logger.error("Error getting favorites", e);
      throw new DrillRuntimeException("Failed to get favorites: " + e.getMessage(), e);
    }
  }

  @POST
  @Path("/{id}/favorite")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Toggle favorite", description = "Toggles a dashboard as favorite for the current user")
  public Response toggleFavorite(
      @Parameter(description = "Dashboard ID") @PathParam("id") String id) {
    logger.debug("Toggling favorite for dashboard: {} by user: {}", id, getCurrentUser());

    try {
      PersistentStore<UserFavorites> store = getFavoritesStore();
      String user = getCurrentUser();
      UserFavorites favorites = store.get(user);

      if (favorites == null) {
        favorites = new UserFavorites();
      }

      boolean nowFavorited;
      if (favorites.getDashboardIds().contains(id)) {
        favorites.getDashboardIds().remove(id);
        nowFavorited = false;
      } else {
        favorites.getDashboardIds().add(id);
        nowFavorited = true;
      }

      store.put(user, favorites);

      String msg = nowFavorited ? "Dashboard added to favorites" : "Dashboard removed from favorites";
      return Response.ok(new FavoriteResponse(nowFavorited, msg)).build();
    } catch (Exception e) {
      logger.error("Error toggling favorite", e);
      throw new DrillRuntimeException("Failed to toggle favorite: " + e.getMessage(), e);
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

  private PersistentStore<UserFavorites> getFavoritesStore() {
    if (cachedFavoritesStore == null) {
      synchronized (DashboardResources.class) {
        if (cachedFavoritesStore == null) {
          try {
            cachedFavoritesStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    UserFavorites.class
                )
                .name(FAVORITES_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access favorites store", e);
          }
        }
      }
    }
    return cachedFavoritesStore;
  }

  private File getUploadDir() {
    if (cachedUploadDir == null) {
      synchronized (DashboardResources.class) {
        if (cachedUploadDir == null) {
          String basePath = System.getenv("DRILL_LOG_DIR");
          if (basePath == null) {
            basePath = System.getProperty("java.io.tmpdir");
          }
          File dir = new File(basePath, UPLOAD_DIR_NAME);
          if (!dir.exists() && !dir.mkdirs()) {
            throw new DrillRuntimeException("Failed to create upload directory: " + dir);
          }
          cachedUploadDir = dir;
        }
      }
    }
    return cachedUploadDir;
  }

  private static String getFileExtension(String filename) {
    int dotIndex = filename.lastIndexOf('.');
    if (dotIndex < 0 || dotIndex == filename.length() - 1) {
      return "";
    }
    return filename.substring(dotIndex + 1).toLowerCase();
  }

  private String getCurrentUser() {
    return principal.getName();
  }
}
