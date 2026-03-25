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
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for managing projects.
 * Projects are the top-level organizational unit containing datasets,
 * saved queries, visualizations, dashboards, and wiki documentation.
 */
@Path("/api/v1/projects")
@Tag(name = "Projects", description = "APIs for managing data analytics projects")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ProjectResources {
  private static final Logger logger = LoggerFactory.getLogger(ProjectResources.class);
  private static final String STORE_NAME = "drill.sqllab.projects";
  private static final String FAVORITES_STORE_NAME = "drill.sqllab.project_favorites";

  public static final String SYSTEM_LOGS_PROJECT_ID = "system-drill-logs";

  private static final String DFS_PLUGIN_NAME = "dfs";
  private static final String LOGS_WORKSPACE_NAME = "logs";
  private static final String DRILL_LOG_FORMAT_NAME = "drilllog";

  // Regex to parse Drill's default logback format:
  // %date{ISO8601} [%thread] %-5level %logger{36} - %msg%n
  private static final String DRILL_LOG_REGEX =
      "(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2},\\d+)\\s+\\[([^\\]]+)\\]\\s+(\\w+)\\s+([^\\s]+)\\s+-\\s+(.*)";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<Project> cachedStore;
  private static volatile PersistentStore<UserFavorites> cachedFavoritesStore;

  // ==================== Model Classes ====================

  /**
   * A reference to a dataset (table or saved query) within a project.
   */
  public static class DatasetRef {
    @JsonProperty
    private String id;
    @JsonProperty
    private String type;
    @JsonProperty
    private String schema;
    @JsonProperty
    private String table;
    @JsonProperty
    private String savedQueryId;
    @JsonProperty
    private String label;

    public DatasetRef() {
    }

    @JsonCreator
    public DatasetRef(
        @JsonProperty("id") String id,
        @JsonProperty("type") String type,
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("savedQueryId") String savedQueryId,
        @JsonProperty("label") String label) {
      this.id = id;
      this.type = type != null ? type : "table";
      this.schema = schema;
      this.table = table;
      this.savedQueryId = savedQueryId;
      this.label = label;
    }

    public String getId() { return id; }
    public String getType() { return type; }
    public String getSchema() { return schema; }
    public String getTable() { return table; }
    public String getSavedQueryId() { return savedQueryId; }
    public String getLabel() { return label; }

    public void setId(String id) { this.id = id; }
    public void setType(String type) { this.type = type; }
    public void setSchema(String schema) { this.schema = schema; }
    public void setTable(String table) { this.table = table; }
    public void setSavedQueryId(String savedQueryId) { this.savedQueryId = savedQueryId; }
    public void setLabel(String label) { this.label = label; }
  }

  /**
   * A wiki page within a project for documentation.
   */
  public static class WikiPage {
    @JsonProperty
    private String id;
    @JsonProperty
    private String title;
    @JsonProperty
    private String content;
    @JsonProperty
    private int order;
    @JsonProperty
    private long createdAt;
    @JsonProperty
    private long updatedAt;

    public WikiPage() {
    }

    @JsonCreator
    public WikiPage(
        @JsonProperty("id") String id,
        @JsonProperty("title") String title,
        @JsonProperty("content") String content,
        @JsonProperty("order") int order,
        @JsonProperty("createdAt") long createdAt,
        @JsonProperty("updatedAt") long updatedAt) {
      this.id = id;
      this.title = title;
      this.content = content;
      this.order = order;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
    }

    public String getId() { return id; }
    public String getTitle() { return title; }
    public String getContent() { return content; }
    public int getOrder() { return order; }
    public long getCreatedAt() { return createdAt; }
    public long getUpdatedAt() { return updatedAt; }

    public void setId(String id) { this.id = id; }
    public void setTitle(String title) { this.title = title; }
    public void setContent(String content) { this.content = content; }
    public void setOrder(int order) { this.order = order; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
  }

  /**
   * Project model for persistence.
   */
  public static class Project {
    @JsonProperty
    private String id;
    @JsonProperty
    private String name;
    @JsonProperty
    private String description;
    @JsonProperty
    private List<String> tags;
    @JsonProperty
    private String owner;
    @JsonProperty
    private boolean isPublic;
    @JsonProperty
    private List<String> sharedWith;
    @JsonProperty
    private List<DatasetRef> datasets;
    @JsonProperty
    private List<String> savedQueryIds;
    @JsonProperty
    private List<String> visualizationIds;
    @JsonProperty
    private List<String> dashboardIds;
    @JsonProperty
    private List<WikiPage> wikiPages;
    @JsonProperty
    private boolean isSystem;
    @JsonProperty
    private long createdAt;
    @JsonProperty
    private long updatedAt;

    public Project() {
    }

    @JsonCreator
    public Project(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("tags") List<String> tags,
        @JsonProperty("owner") String owner,
        @JsonProperty("isPublic") boolean isPublic,
        @JsonProperty("sharedWith") List<String> sharedWith,
        @JsonProperty("datasets") List<DatasetRef> datasets,
        @JsonProperty("savedQueryIds") List<String> savedQueryIds,
        @JsonProperty("visualizationIds") List<String> visualizationIds,
        @JsonProperty("dashboardIds") List<String> dashboardIds,
        @JsonProperty("wikiPages") List<WikiPage> wikiPages,
        @JsonProperty("isSystem") boolean isSystem,
        @JsonProperty("createdAt") long createdAt,
        @JsonProperty("updatedAt") long updatedAt) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.tags = tags != null ? tags : new ArrayList<>();
      this.owner = owner;
      this.isPublic = isPublic;
      this.sharedWith = sharedWith != null ? sharedWith : new ArrayList<>();
      this.datasets = datasets != null ? datasets : new ArrayList<>();
      this.savedQueryIds = savedQueryIds != null ? savedQueryIds : new ArrayList<>();
      this.visualizationIds = visualizationIds != null ? visualizationIds : new ArrayList<>();
      this.dashboardIds = dashboardIds != null ? dashboardIds : new ArrayList<>();
      this.wikiPages = wikiPages != null ? wikiPages : new ArrayList<>();
      this.isSystem = isSystem;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getDescription() { return description; }
    public List<String> getTags() { return tags; }
    public String getOwner() { return owner; }
    public boolean isPublic() { return isPublic; }
    public List<String> getSharedWith() { return sharedWith; }
    public List<DatasetRef> getDatasets() { return datasets; }
    public List<String> getSavedQueryIds() { return savedQueryIds; }
    public List<String> getVisualizationIds() { return visualizationIds; }
    public List<String> getDashboardIds() { return dashboardIds; }
    public List<WikiPage> getWikiPages() { return wikiPages; }
    public boolean isSystem() { return isSystem; }
    public long getCreatedAt() { return createdAt; }
    public long getUpdatedAt() { return updatedAt; }

    public void setName(String name) { this.name = name; }
    public void setDescription(String description) { this.description = description; }
    public void setTags(List<String> tags) { this.tags = tags; }
    public void setPublic(boolean isPublic) { this.isPublic = isPublic; }
    public void setSharedWith(List<String> sharedWith) { this.sharedWith = sharedWith; }
    public void setDatasets(List<DatasetRef> datasets) { this.datasets = datasets; }
    public void setSavedQueryIds(List<String> savedQueryIds) { this.savedQueryIds = savedQueryIds; }
    public void setVisualizationIds(List<String> visualizationIds) { this.visualizationIds = visualizationIds; }
    public void setDashboardIds(List<String> dashboardIds) { this.dashboardIds = dashboardIds; }
    public void setWikiPages(List<WikiPage> wikiPages) { this.wikiPages = wikiPages; }
    public void setSystem(boolean isSystem) { this.isSystem = isSystem; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
  }

  /**
   * Stores a user's list of favorited project IDs.
   */
  public static class UserFavorites {
    @JsonProperty
    private List<String> projectIds;

    public UserFavorites() {
      this.projectIds = new ArrayList<>();
    }

    @JsonCreator
    public UserFavorites(
        @JsonProperty("projectIds") List<String> projectIds) {
      this.projectIds = projectIds != null ? projectIds : new ArrayList<>();
    }

    public List<String> getProjectIds() { return projectIds; }
    public void setProjectIds(List<String> projectIds) { this.projectIds = projectIds; }
  }

  // ==================== Request/Response Classes ====================

  /**
   * Request body for creating a new project.
   */
  public static class CreateProjectRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public List<String> tags;
    @JsonProperty
    public boolean isPublic;
  }

  /**
   * Request body for updating a project.
   */
  public static class UpdateProjectRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public List<String> tags;
    @JsonProperty
    public Boolean isPublic;
    @JsonProperty
    public List<String> sharedWith;
  }

  /**
   * Request body for creating/updating a wiki page.
   */
  public static class WikiPageRequest {
    @JsonProperty
    public String title;
    @JsonProperty
    public String content;
    @JsonProperty
    public Integer order;
  }

  /**
   * Response containing a list of projects.
   */
  public static class ProjectsResponse {
    @JsonProperty
    public List<Project> projects;

    public ProjectsResponse(List<Project> projects) {
      this.projects = projects;
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
   * Response containing a list of favorited project IDs.
   */
  public static class FavoritesListResponse {
    @JsonProperty
    public List<String> projectIds;

    public FavoritesListResponse(List<String> projectIds) {
      this.projectIds = projectIds;
    }
  }

  // ==================== CRUD Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List projects",
      description = "Returns all projects accessible by the current user")
  public ProjectsResponse listProjects() {
    logger.debug("Listing projects for user: {}", getCurrentUser());
    ensureSystemProject();

    List<Project> projects = new ArrayList<>();
    String currentUser = getCurrentUser();

    try {
      PersistentStore<Project> store = getStore();
      Iterator<Map.Entry<String, Project>> iterator = store.getAll();

      while (iterator.hasNext()) {
        Map.Entry<String, Project> entry = iterator.next();
        Project project = entry.getValue();

        if (canRead(project, currentUser)) {
          projects.add(project);
        }
      }
    } catch (Exception e) {
      logger.error("Error listing projects", e);
      throw new DrillRuntimeException("Failed to list projects: " + e.getMessage(), e);
    }

    return new ProjectsResponse(projects);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create project", description = "Creates a new project")
  public Response createProject(CreateProjectRequest request) {
    logger.debug("Creating project: {}", request.name);

    if (request.name == null || request.name.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("Project name is required"))
          .build();
    }

    String id = UUID.randomUUID().toString();
    long now = Instant.now().toEpochMilli();

    Project project = new Project(
        id,
        request.name.trim(),
        request.description,
        request.tags,
        getCurrentUser(),
        request.isPublic,
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        false,
        now,
        now
    );

    try {
      PersistentStore<Project> store = getStore();
      store.put(id, project);
    } catch (Exception e) {
      logger.error("Error creating project", e);
      throw new DrillRuntimeException("Failed to create project: " + e.getMessage(), e);
    }

    return Response.status(Response.Status.CREATED).entity(project).build();
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get project", description = "Returns a project by ID")
  public Response getProject(
      @Parameter(description = "Project ID") @PathParam("id") String id) {
    logger.debug("Getting project: {}", id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!canRead(project, getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Access denied"))
            .build();
      }

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error getting project", e);
      throw new DrillRuntimeException("Failed to get project: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update project", description = "Updates an existing project")
  public Response updateProject(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      UpdateProjectRequest request) {
    logger.debug("Updating project: {}", id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can update this project"))
            .build();
      }

      if (request.name != null) {
        project.setName(request.name.trim());
      }
      if (request.description != null) {
        project.setDescription(request.description);
      }
      if (request.tags != null) {
        project.setTags(request.tags);
      }
      if (request.isPublic != null) {
        project.setPublic(request.isPublic);
      }
      if (request.sharedWith != null) {
        project.setSharedWith(request.sharedWith);
      }

      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error updating project", e);
      throw new DrillRuntimeException("Failed to update project: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete project", description = "Deletes a project")
  public Response deleteProject(
      @Parameter(description = "Project ID") @PathParam("id") String id) {
    logger.debug("Deleting project: {}", id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (project.isSystem()) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("System projects cannot be deleted"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can delete this project"))
            .build();
      }

      store.delete(id);

      return Response.ok(new MessageResponse("Project deleted successfully")).build();
    } catch (Exception e) {
      logger.error("Error deleting project", e);
      throw new DrillRuntimeException("Failed to delete project: " + e.getMessage(), e);
    }
  }

  // ==================== Dataset Endpoints ====================

  @POST
  @Path("/{id}/datasets")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Add dataset", description = "Adds a dataset reference to a project")
  public Response addDataset(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      DatasetRef datasetRef) {
    logger.debug("Adding dataset to project: {}", id);

    try {
      PersistentStore<Project> store = getStore();

      // Synchronize on the interned project ID to prevent concurrent
      // read-modify-write races on the same project.
      synchronized (id.intern()) {
        Project project = store.get(id);

        if (project == null) {
          return Response.status(Response.Status.NOT_FOUND)
              .entity(new MessageResponse("Project not found"))
              .build();
        }

        if (!project.getOwner().equals(getCurrentUser())) {
          return Response.status(Response.Status.FORBIDDEN)
              .entity(new MessageResponse("Only the owner can modify this project"))
              .build();
        }

        if (datasetRef.getId() == null || datasetRef.getId().isEmpty()) {
          datasetRef.setId(UUID.randomUUID().toString());
        }

        project.getDatasets().add(datasetRef);
        project.setUpdatedAt(Instant.now().toEpochMilli());
        store.put(id, project);

        return Response.ok(project).build();
      }
    } catch (Exception e) {
      logger.error("Error adding dataset to project", e);
      throw new DrillRuntimeException("Failed to add dataset: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}/datasets/{datasetId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Remove dataset", description = "Removes a dataset reference from a project")
  public Response removeDataset(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Dataset ID") @PathParam("datasetId") String datasetId) {
    logger.debug("Removing dataset {} from project: {}", datasetId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      project.getDatasets().removeIf(d -> d.getId().equals(datasetId));
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error removing dataset from project", e);
      throw new DrillRuntimeException("Failed to remove dataset: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/cleanup/plugin/{pluginName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Clean up plugin datasets",
      description = "Removes all dataset references for a deleted plugin from all projects")
  public Response cleanupPluginDatasets(
      @Parameter(description = "Plugin name") @PathParam("pluginName") String pluginName) {
    logger.debug("Cleaning up datasets for deleted plugin: {}", pluginName);

    try {
      PersistentStore<Project> store = getStore();
      Iterator<Map.Entry<String, Project>> iterator = store.getAll();
      int removedCount = 0;

      while (iterator.hasNext()) {
        Map.Entry<String, Project> entry = iterator.next();
        Project project = entry.getValue();
        List<DatasetRef> datasets = project.getDatasets();

        int before = datasets.size();
        datasets.removeIf(d -> {
          if (d.getSchema() == null) {
            return false;
          }
          String dsPlugin = d.getSchema().split("\\.")[0];
          return dsPlugin.equals(pluginName);
        });

        if (datasets.size() < before) {
          removedCount += before - datasets.size();
          project.setUpdatedAt(Instant.now().toEpochMilli());
          store.put(entry.getKey(), project);
        }
      }

      return Response.ok(new MessageResponse(
          "Removed " + removedCount + " dataset reference(s) for plugin " + pluginName
      )).build();
    } catch (Exception e) {
      logger.error("Error cleaning up plugin datasets", e);
      throw new DrillRuntimeException("Failed to clean up plugin datasets: " + e.getMessage(), e);
    }
  }

  // ==================== Saved Query Endpoints ====================

  @POST
  @Path("/{id}/saved-queries/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Add saved query",
      description = "Adds a saved query to a project, removing it from any other project")
  public Response addSavedQuery(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Saved Query ID") @PathParam("queryId") String queryId) {
    logger.debug("Adding saved query {} to project: {}", queryId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      // Remove from all other projects first
      removeItemFromAllProjects(store, "savedQuery", queryId, id);

      if (!project.getSavedQueryIds().contains(queryId)) {
        project.getSavedQueryIds().add(queryId);
      }
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error adding saved query to project", e);
      throw new DrillRuntimeException("Failed to add saved query: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}/saved-queries/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Remove saved query",
      description = "Removes a saved query from a project")
  public Response removeSavedQuery(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Saved Query ID") @PathParam("queryId") String queryId) {
    logger.debug("Removing saved query {} from project: {}", queryId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      project.getSavedQueryIds().remove(queryId);
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error removing saved query from project", e);
      throw new DrillRuntimeException("Failed to remove saved query: " + e.getMessage(), e);
    }
  }

  // ==================== Visualization Endpoints ====================

  @POST
  @Path("/{id}/visualizations/{vizId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Add visualization",
      description = "Adds a visualization to a project, removing it from any other project")
  public Response addVisualization(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Visualization ID") @PathParam("vizId") String vizId) {
    logger.debug("Adding visualization {} to project: {}", vizId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      removeItemFromAllProjects(store, "visualization", vizId, id);

      if (!project.getVisualizationIds().contains(vizId)) {
        project.getVisualizationIds().add(vizId);
      }
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error adding visualization to project", e);
      throw new DrillRuntimeException("Failed to add visualization: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}/visualizations/{vizId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Remove visualization",
      description = "Removes a visualization from a project")
  public Response removeVisualization(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Visualization ID") @PathParam("vizId") String vizId) {
    logger.debug("Removing visualization {} from project: {}", vizId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      project.getVisualizationIds().remove(vizId);
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error removing visualization from project", e);
      throw new DrillRuntimeException("Failed to remove visualization: " + e.getMessage(), e);
    }
  }

  // ==================== Dashboard Endpoints ====================

  @POST
  @Path("/{id}/dashboards/{dashId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Add dashboard",
      description = "Adds a dashboard to a project, removing it from any other project")
  public Response addDashboard(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Dashboard ID") @PathParam("dashId") String dashId) {
    logger.debug("Adding dashboard {} to project: {}", dashId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      removeItemFromAllProjects(store, "dashboard", dashId, id);

      if (!project.getDashboardIds().contains(dashId)) {
        project.getDashboardIds().add(dashId);
      }
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error adding dashboard to project", e);
      throw new DrillRuntimeException("Failed to add dashboard: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}/dashboards/{dashId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Remove dashboard",
      description = "Removes a dashboard from a project")
  public Response removeDashboard(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Dashboard ID") @PathParam("dashId") String dashId) {
    logger.debug("Removing dashboard {} from project: {}", dashId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      project.getDashboardIds().remove(dashId);
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(project).build();
    } catch (Exception e) {
      logger.error("Error removing dashboard from project", e);
      throw new DrillRuntimeException("Failed to remove dashboard: " + e.getMessage(), e);
    }
  }

  // ==================== Wiki Endpoints ====================

  @POST
  @Path("/{id}/wiki")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create wiki page",
      description = "Creates a new wiki page in a project")
  public Response createWikiPage(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      WikiPageRequest request) {
    logger.debug("Creating wiki page in project: {}", id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      if (request.title == null || request.title.trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new MessageResponse("Wiki page title is required"))
            .build();
      }

      long now = Instant.now().toEpochMilli();
      WikiPage page = new WikiPage(
          UUID.randomUUID().toString(),
          request.title.trim(),
          request.content != null ? request.content : "",
          request.order != null ? request.order : project.getWikiPages().size(),
          now,
          now
      );

      project.getWikiPages().add(page);
      project.setUpdatedAt(now);
      store.put(id, project);

      return Response.status(Response.Status.CREATED).entity(page).build();
    } catch (Exception e) {
      logger.error("Error creating wiki page", e);
      throw new DrillRuntimeException("Failed to create wiki page: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}/wiki/{pageId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update wiki page",
      description = "Updates a wiki page in a project")
  public Response updateWikiPage(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Wiki Page ID") @PathParam("pageId") String pageId,
      WikiPageRequest request) {
    logger.debug("Updating wiki page {} in project: {}", pageId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      WikiPage target = null;
      for (WikiPage page : project.getWikiPages()) {
        if (page.getId().equals(pageId)) {
          target = page;
          break;
        }
      }

      if (target == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Wiki page not found"))
            .build();
      }

      if (request.title != null) {
        target.setTitle(request.title.trim());
      }
      if (request.content != null) {
        target.setContent(request.content);
      }
      if (request.order != null) {
        target.setOrder(request.order);
      }

      long now = Instant.now().toEpochMilli();
      target.setUpdatedAt(now);
      project.setUpdatedAt(now);
      store.put(id, project);

      return Response.ok(target).build();
    } catch (Exception e) {
      logger.error("Error updating wiki page", e);
      throw new DrillRuntimeException("Failed to update wiki page: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}/wiki/{pageId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete wiki page",
      description = "Deletes a wiki page from a project")
  public Response deleteWikiPage(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Wiki Page ID") @PathParam("pageId") String pageId) {
    logger.debug("Deleting wiki page {} from project: {}", pageId, id);

    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);

      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found"))
            .build();
      }

      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project"))
            .build();
      }

      project.getWikiPages().removeIf(p -> p.getId().equals(pageId));
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      return Response.ok(new MessageResponse("Wiki page deleted successfully")).build();
    } catch (Exception e) {
      logger.error("Error deleting wiki page", e);
      throw new DrillRuntimeException("Failed to delete wiki page: " + e.getMessage(), e);
    }
  }

  // ==================== Favorites Endpoints ====================

  @GET
  @Path("/favorites")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get favorites",
      description = "Returns the current user's favorited project IDs")
  public FavoritesListResponse getFavorites() {
    logger.debug("Getting favorites for user: {}", getCurrentUser());

    try {
      PersistentStore<UserFavorites> store = getFavoritesStore();
      UserFavorites favorites = store.get(getCurrentUser());

      if (favorites == null) {
        return new FavoritesListResponse(new ArrayList<>());
      }

      return new FavoritesListResponse(favorites.getProjectIds());
    } catch (Exception e) {
      logger.error("Error getting favorites", e);
      throw new DrillRuntimeException("Failed to get favorites: " + e.getMessage(), e);
    }
  }

  @POST
  @Path("/{id}/favorite")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Toggle favorite",
      description = "Toggles a project as favorite for the current user")
  public Response toggleFavorite(
      @Parameter(description = "Project ID") @PathParam("id") String id) {
    logger.debug("Toggling favorite for project: {} by user: {}", id, getCurrentUser());

    try {
      PersistentStore<UserFavorites> store = getFavoritesStore();
      String user = getCurrentUser();
      UserFavorites favorites = store.get(user);

      if (favorites == null) {
        favorites = new UserFavorites();
      }

      boolean nowFavorited;
      if (favorites.getProjectIds().contains(id)) {
        favorites.getProjectIds().remove(id);
        nowFavorited = false;
      } else {
        favorites.getProjectIds().add(id);
        nowFavorited = true;
      }

      store.put(user, favorites);

      String msg = nowFavorited
          ? "Project added to favorites"
          : "Project removed from favorites";
      return Response.ok(new FavoriteResponse(nowFavorited, msg)).build();
    } catch (Exception e) {
      logger.error("Error toggling favorite", e);
      throw new DrillRuntimeException("Failed to toggle favorite: " + e.getMessage(), e);
    }
  }

  // ==================== Helper Methods ====================

  private boolean canRead(Project project, String user) {
    return project.getOwner().equals(user)
        || project.isPublic()
        || project.getSharedWith().contains(user);
  }

  /**
   * Removes an item ID from all projects except the target project.
   * This ensures single-project ownership for saved queries, visualizations, and dashboards.
   */
  private void removeItemFromAllProjects(
      PersistentStore<Project> store, String itemType, String itemId, String excludeProjectId) {
    Iterator<Map.Entry<String, Project>> iterator = store.getAll();
    while (iterator.hasNext()) {
      Map.Entry<String, Project> entry = iterator.next();
      Project p = entry.getValue();

      if (p.getId().equals(excludeProjectId)) {
        continue;
      }

      boolean modified = false;
      switch (itemType) {
        case "savedQuery":
          modified = p.getSavedQueryIds().remove(itemId);
          break;
        case "visualization":
          modified = p.getVisualizationIds().remove(itemId);
          break;
        case "dashboard":
          modified = p.getDashboardIds().remove(itemId);
          break;
        default:
          break;
      }

      if (modified) {
        p.setUpdatedAt(Instant.now().toEpochMilli());
        store.put(entry.getKey(), p);
      }
    }
  }

  /**
   * Ensures the system "Drill Logs" project exists with pre-built
   * saved queries, visualizations, and a dashboard for log analysis.
   * Also auto-configures the dfs.logs workspace and drilllog format
   * if DRILL_LOG_DIR is set.
   */
  private void ensureSystemProject() {
    try {
      PersistentStore<Project> store = getStore();
      Project existing = store.get(SYSTEM_LOGS_PROJECT_ID);
      if (existing != null) {
        return;
      }

      // Auto-configure the dfs.logs workspace and format plugin
      ensureLogWorkspace();

      long now = Instant.now().toEpochMilli();
      String owner = "system";

      // Define SQL for saved queries (reused in visualizations)
      String sqlRecentErrors = "SELECT log_timestamp, logger, message\n"
          + "FROM dfs.logs.`*.drilllog`\n"
          + "WHERE level = 'ERROR'\n"
          + "ORDER BY log_timestamp DESC\nLIMIT 100";

      String sqlErrorFrequency = "SELECT SUBSTR(log_timestamp, 1, 13) AS hour,\n"
          + "       COUNT(*) AS error_count\n"
          + "FROM dfs.logs.`*.drilllog`\n"
          + "WHERE level = 'ERROR'\n"
          + "GROUP BY SUBSTR(log_timestamp, 1, 13)\n"
          + "ORDER BY hour DESC\nLIMIT 48";

      String sqlLevelDist = "SELECT level, COUNT(*) AS cnt\n"
          + "FROM dfs.logs.`*.drilllog`\n"
          + "GROUP BY level\nORDER BY cnt DESC";

      String sqlTopLoggers = "SELECT logger, COUNT(*) AS error_count\n"
          + "FROM dfs.logs.`*.drilllog`\n"
          + "WHERE level = 'ERROR'\n"
          + "GROUP BY logger\n"
          + "ORDER BY error_count DESC\nLIMIT 20";

      String sqlWarnTrends = "SELECT SUBSTR(log_timestamp, 1, 10) AS day,\n"
          + "       COUNT(*) AS warn_count\n"
          + "FROM dfs.logs.`*.drilllog`\n"
          + "WHERE level = 'WARN'\n"
          + "GROUP BY SUBSTR(log_timestamp, 1, 10)\n"
          + "ORDER BY day DESC\nLIMIT 30";

      // Create saved queries
      PersistentStore<SavedQueryResources.SavedQuery> sqStore = storeProvider.getOrCreateStore(
          PersistentStoreConfig.newJacksonBuilder(
              workManager.getContext().getLpPersistence().getMapper(),
              SavedQueryResources.SavedQuery.class
          ).name("drill.sqllab.saved_queries").build()
      );

      String sqRecentErrors = "sys-log-recent-errors";
      String sqErrorFrequency = "sys-log-error-frequency";
      String sqLevelDist = "sys-log-level-dist";
      String sqTopLoggers = "sys-log-top-loggers";
      String sqWarnTrends = "sys-log-warn-trends";

      if (sqStore.get(sqRecentErrors) == null) {
        sqStore.put(sqRecentErrors, new SavedQueryResources.SavedQuery(
            sqRecentErrors, "Recent Errors",
            "Shows the most recent ERROR log entries",
            sqlRecentErrors,
            "dfs.logs", owner, now, now, new HashMap<>(), true));
      }

      if (sqStore.get(sqErrorFrequency) == null) {
        sqStore.put(sqErrorFrequency, new SavedQueryResources.SavedQuery(
            sqErrorFrequency, "Error Frequency by Hour",
            "Counts errors per hour for trend analysis",
            sqlErrorFrequency,
            "dfs.logs", owner, now, now, new HashMap<>(), true));
      }

      if (sqStore.get(sqLevelDist) == null) {
        sqStore.put(sqLevelDist, new SavedQueryResources.SavedQuery(
            sqLevelDist, "Log Level Distribution",
            "Distribution of log entries by level",
            sqlLevelDist,
            "dfs.logs", owner, now, now, new HashMap<>(), true));
      }

      if (sqStore.get(sqTopLoggers) == null) {
        sqStore.put(sqTopLoggers, new SavedQueryResources.SavedQuery(
            sqTopLoggers, "Top Error Sources",
            "Loggers producing the most errors",
            sqlTopLoggers,
            "dfs.logs", owner, now, now, new HashMap<>(), true));
      }

      if (sqStore.get(sqWarnTrends) == null) {
        sqStore.put(sqWarnTrends, new SavedQueryResources.SavedQuery(
            sqWarnTrends, "Warning Trends",
            "Daily warning counts for trend analysis",
            sqlWarnTrends,
            "dfs.logs", owner, now, now, new HashMap<>(), true));
      }

      // Create visualizations (with SQL populated for dashboard rendering)
      PersistentStore<VisualizationResources.Visualization> vizStore =
          storeProvider.getOrCreateStore(
              PersistentStoreConfig.newJacksonBuilder(
                  workManager.getContext().getLpPersistence().getMapper(),
                  VisualizationResources.Visualization.class
              ).name("drill.sqllab.visualizations").build()
          );

      String vizErrorFreq = "sys-viz-error-frequency";
      String vizLevelDist = "sys-viz-level-dist";
      String vizTopLoggers = "sys-viz-top-loggers";
      String vizWarnTrends = "sys-viz-warn-trends";

      if (vizStore.get(vizErrorFreq) == null) {
        vizStore.put(vizErrorFreq, new VisualizationResources.Visualization(
            vizErrorFreq, "Error Frequency Over Time",
            "Hourly error count trend",
            sqErrorFrequency, "line",
            new VisualizationResources.VisualizationConfig(
                "hour", "error_count", null, null, null, null),
            owner, now, now, true, sqlErrorFrequency, "dfs.logs", null));
      }

      if (vizStore.get(vizLevelDist) == null) {
        vizStore.put(vizLevelDist, new VisualizationResources.Visualization(
            vizLevelDist, "Log Level Distribution",
            "Pie chart showing log level breakdown",
            sqLevelDist, "pie",
            new VisualizationResources.VisualizationConfig(
                null, null,
                Arrays.asList("cnt"),
                Arrays.asList("level"),
                null, null),
            owner, now, now, true, sqlLevelDist, "dfs.logs", null));
      }

      if (vizStore.get(vizTopLoggers) == null) {
        vizStore.put(vizTopLoggers, new VisualizationResources.Visualization(
            vizTopLoggers, "Top Error Sources",
            "Bar chart of loggers producing the most errors",
            sqTopLoggers, "bar",
            new VisualizationResources.VisualizationConfig(
                "logger", "error_count", null, null, null, null),
            owner, now, now, true, sqlTopLoggers, "dfs.logs", null));
      }

      if (vizStore.get(vizWarnTrends) == null) {
        vizStore.put(vizWarnTrends, new VisualizationResources.Visualization(
            vizWarnTrends, "Warning Trends",
            "Daily warning count trend",
            sqWarnTrends, "line",
            new VisualizationResources.VisualizationConfig(
                "day", "warn_count", null, null, null, null),
            owner, now, now, true, sqlWarnTrends, "dfs.logs", null));
      }

      // Create dashboard
      PersistentStore<DashboardResources.Dashboard> dashStore =
          storeProvider.getOrCreateStore(
              PersistentStoreConfig.newJacksonBuilder(
                  workManager.getContext().getLpPersistence().getMapper(),
                  DashboardResources.Dashboard.class
              ).name("drill.sqllab.dashboards").build()
          );

      String dashId = "sys-dash-logs-overview";
      if (dashStore.get(dashId) == null) {
        List<DashboardResources.DashboardPanel> panels = new ArrayList<>();
        panels.add(new DashboardResources.DashboardPanel(
            "p1", "visualization", vizErrorFreq, null, null, null, 0, 0, 6, 4));
        panels.add(new DashboardResources.DashboardPanel(
            "p2", "visualization", vizLevelDist, null, null, null, 6, 0, 6, 4));
        panels.add(new DashboardResources.DashboardPanel(
            "p3", "visualization", vizTopLoggers, null, null, null, 0, 4, 6, 4));
        panels.add(new DashboardResources.DashboardPanel(
            "p4", "visualization", vizWarnTrends, null, null, null, 6, 4, 6, 4));

        dashStore.put(dashId, new DashboardResources.Dashboard(
            dashId, "Drill Logs Overview",
            "Pre-built dashboard for monitoring Drill server logs",
            panels, null, null, owner, now, now, 0, true));
      }

      // Create the system project
      List<DatasetRef> datasets = new ArrayList<>();
      datasets.add(new DatasetRef(
          "sys-ds-drill-logs", "schema",
          "dfs.logs", null, null, "Drill Log Files"));

      Project project = new Project(
          SYSTEM_LOGS_PROJECT_ID,
          "Drill Logs",
          "System project for analyzing Apache Drill server logs. "
              + "Includes pre-built queries, visualizations, and dashboards.",
          Arrays.asList("system", "logs", "monitoring"),
          owner,
          true,
          new ArrayList<>(),
          datasets,
          Arrays.asList(sqRecentErrors, sqErrorFrequency, sqLevelDist,
              sqTopLoggers, sqWarnTrends),
          Arrays.asList(vizErrorFreq, vizLevelDist, vizTopLoggers, vizWarnTrends),
          Arrays.asList(dashId),
          new ArrayList<>(),
          true,
          now,
          now
      );

      store.put(SYSTEM_LOGS_PROJECT_ID, project);
      logger.info("Created system 'Drill Logs' project with pre-built analytics");

    } catch (Exception e) {
      logger.warn("Failed to create system Drill Logs project: {}", e.getMessage());
    }
  }

  /**
   * Configures the dfs.logs workspace and drilllog format plugin
   * if DRILL_LOG_DIR is set and they don't already exist.
   */
  private void ensureLogWorkspace() {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      logger.debug("DRILL_LOG_DIR not set, skipping log workspace setup");
      return;
    }

    try {
      StoragePluginRegistry storage = workManager.getContext().getStorage();
      StoragePluginConfig config = storage.getStoredConfig(DFS_PLUGIN_NAME);

      if (!(config instanceof FileSystemConfig)) {
        return;
      }

      FileSystemConfig fsConfig = (FileSystemConfig) config;

      // Add workspace if missing
      if (!fsConfig.getWorkspaces().containsKey(LOGS_WORKSPACE_NAME)) {
        FileSystemConfig copy = fsConfig.copy();
        copy.getWorkspaces().put(LOGS_WORKSPACE_NAME,
            new WorkspaceConfig(logDir, false, DRILL_LOG_FORMAT_NAME, false));
        storage.put(DFS_PLUGIN_NAME, copy);
        logger.info("Created dfs.logs workspace pointing to {}", logDir);
      }

      // Add format plugin if missing
      StoragePluginConfig updatedConfig = storage.getStoredConfig(DFS_PLUGIN_NAME);
      if (updatedConfig instanceof FileSystemConfig) {
        FileSystemConfig updatedFsConfig = (FileSystemConfig) updatedConfig;
        if (!updatedFsConfig.getFormats().containsKey(DRILL_LOG_FORMAT_NAME)) {
          String formatJson = "{"
              + "\"type\": \"logRegex\","
              + "\"regex\": \"" + DRILL_LOG_REGEX.replace("\\", "\\\\") + "\","
              + "\"extension\": \"drilllog\","
              + "\"maxErrors\": 100000,"
              + "\"schema\": ["
              + "  {\"fieldName\": \"log_timestamp\", \"fieldType\": \"VARCHAR\"},"
              + "  {\"fieldName\": \"thread\", \"fieldType\": \"VARCHAR\"},"
              + "  {\"fieldName\": \"level\", \"fieldType\": \"VARCHAR\"},"
              + "  {\"fieldName\": \"logger\", \"fieldType\": \"VARCHAR\"},"
              + "  {\"fieldName\": \"message\", \"fieldType\": \"VARCHAR\"}"
              + "]}";

          FormatPluginConfig logFormat = storage.mapper()
              .readValue(formatJson, FormatPluginConfig.class);
          storage.putFormatPlugin(DFS_PLUGIN_NAME, DRILL_LOG_FORMAT_NAME, logFormat);
          logger.info("Created drilllog format plugin for parsing Drill logs");
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to auto-configure log workspace: {}", e.getMessage());
    }
  }

  private PersistentStore<Project> getStore() {
    if (cachedStore == null) {
      synchronized (ProjectResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    Project.class
                )
                .name(STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access projects store", e);
          }
        }
      }
    }
    return cachedStore;
  }

  private PersistentStore<UserFavorites> getFavoritesStore() {
    if (cachedFavoritesStore == null) {
      synchronized (ProjectResources.class) {
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

  private String getCurrentUser() {
    return principal.getName();
  }
}
