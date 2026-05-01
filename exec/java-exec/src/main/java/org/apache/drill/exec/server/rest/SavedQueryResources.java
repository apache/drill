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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for managing saved SQL queries.
 * Queries are persisted using Drill's PersistentStore mechanism.
 */
@Path("/api/v1/saved-queries")
@Tag(name = "Saved Queries", description = "APIs for managing saved SQL queries")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class SavedQueryResources {
  private static final Logger logger = LoggerFactory.getLogger(SavedQueryResources.class);
  private static final String STORE_NAME = "drill.sqllab.saved_queries";
  private static final long TRASH_TTL_MS = 30L * 24 * 60 * 60 * 1000;

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<SavedQuery> cachedStore;

  // ==================== Model Classes ====================

  /**
   * Saved query model for persistence.
   */
  public static class SavedQuery {
    @JsonProperty
    private String id;
    @JsonProperty
    private String name;
    @JsonProperty
    private String description;
    @JsonProperty
    private String sql;
    @JsonProperty
    private String defaultSchema;
    @JsonProperty
    private String owner;
    @JsonProperty
    private long createdAt;
    @JsonProperty
    private long updatedAt;
    @JsonProperty
    private Map<String, String> tags;
    @JsonProperty
    private boolean isPublic;
    @JsonProperty
    private long deletedAt;

    // Default constructor for Jackson
    public SavedQuery() {
    }

    @JsonCreator
    public SavedQuery(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("sql") String sql,
        @JsonProperty("defaultSchema") String defaultSchema,
        @JsonProperty("owner") String owner,
        @JsonProperty("createdAt") long createdAt,
        @JsonProperty("updatedAt") long updatedAt,
        @JsonProperty("tags") Map<String, String> tags,
        @JsonProperty("isPublic") boolean isPublic,
        @JsonProperty("deletedAt") long deletedAt) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.sql = sql;
      this.defaultSchema = defaultSchema;
      this.owner = owner;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.tags = tags;
      this.isPublic = isPublic;
      this.deletedAt = deletedAt;
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

    public String getSql() {
      return sql;
    }

    public String getDefaultSchema() {
      return defaultSchema;
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

    public Map<String, String> getTags() {
      return tags;
    }

    public boolean isPublic() {
      return isPublic;
    }

    public long getDeletedAt() {
      return deletedAt;
    }

    // Setters for updates
    public void setName(String name) {
      this.name = name;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void setSql(String sql) {
      this.sql = sql;
    }

    public void setDefaultSchema(String defaultSchema) {
      this.defaultSchema = defaultSchema;
    }

    public void setUpdatedAt(long updatedAt) {
      this.updatedAt = updatedAt;
    }

    public void setTags(Map<String, String> tags) {
      this.tags = tags;
    }

    public void setPublic(boolean isPublic) {
      this.isPublic = isPublic;
    }

    public void setDeletedAt(long deletedAt) {
      this.deletedAt = deletedAt;
    }
  }

  /**
   * Request body for creating a new saved query.
   */
  public static class CreateSavedQueryRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public String sql;
    @JsonProperty
    public String defaultSchema;
    @JsonProperty
    public Map<String, String> tags;
    @JsonProperty
    public boolean isPublic;
  }

  /**
   * Request body for updating a saved query.
   */
  public static class UpdateSavedQueryRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String description;
    @JsonProperty
    public String sql;
    @JsonProperty
    public String defaultSchema;
    @JsonProperty
    public Map<String, String> tags;
    @JsonProperty
    public Boolean isPublic;
  }

  /**
   * Response containing a list of saved queries.
   */
  public static class SavedQueriesResponse {
    @JsonProperty
    public List<SavedQuery> queries;

    public SavedQueriesResponse(List<SavedQuery> queries) {
      this.queries = queries;
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
  @Operation(summary = "List saved queries", description = "Returns all saved queries accessible by the current user")
  public SavedQueriesResponse listSavedQueries() {
    logger.debug("Listing saved queries for user: {}", getCurrentUser());

    List<SavedQuery> queries = new ArrayList<>();
    String currentUser = getCurrentUser();

    try {
      PersistentStore<SavedQuery> store = getStore();
      Iterator<Map.Entry<String, SavedQuery>> iterator = store.getAll();

      while (iterator.hasNext()) {
        Map.Entry<String, SavedQuery> entry = iterator.next();
        SavedQuery query = entry.getValue();

        if (query.getDeletedAt() > 0) {
          continue;
        }

        // Return queries owned by user or public queries
        if (query.getOwner().equals(currentUser) || query.isPublic()) {
          queries.add(query);
        }
      }
    } catch (Exception e) {
      logger.error("Error listing saved queries", e);
      throw new DrillRuntimeException("Failed to list saved queries: " + e.getMessage(), e);
    }

    return new SavedQueriesResponse(queries);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create saved query", description = "Creates a new saved query")
  public Response createSavedQuery(CreateSavedQueryRequest request) {
    logger.debug("Creating saved query: {}", request.name);

    if (request.name == null || request.name.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("Query name is required"))
          .build();
    }

    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("SQL is required"))
          .build();
    }

    String id = UUID.randomUUID().toString();
    long now = Instant.now().toEpochMilli();

    SavedQuery query = new SavedQuery(
        id,
        request.name.trim(),
        request.description,
        request.sql,
        request.defaultSchema,
        getCurrentUser(),
        now,
        now,
        request.tags != null ? request.tags : new HashMap<>(),
        request.isPublic,
        0L
    );

    try {
      PersistentStore<SavedQuery> store = getStore();
      store.put(id, query);
    } catch (Exception e) {
      logger.error("Error creating saved query", e);
      throw new DrillRuntimeException("Failed to create saved query: " + e.getMessage(), e);
    }

    return Response.status(Response.Status.CREATED).entity(query).build();
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get saved query", description = "Returns a saved query by ID")
  public Response getSavedQuery(
      @Parameter(description = "Query ID") @PathParam("id") String id) {
    logger.debug("Getting saved query: {}", id);

    try {
      PersistentStore<SavedQuery> store = getStore();
      SavedQuery query = store.get(id);

      if (query == null || query.getDeletedAt() > 0) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Query not found"))
            .build();
      }

      // Check access permissions
      if (!query.getOwner().equals(getCurrentUser()) && !query.isPublic()) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Access denied"))
            .build();
      }

      return Response.ok(query).build();
    } catch (Exception e) {
      logger.error("Error getting saved query", e);
      throw new DrillRuntimeException("Failed to get saved query: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update saved query", description = "Updates an existing saved query")
  public Response updateSavedQuery(
      @Parameter(description = "Query ID") @PathParam("id") String id,
      UpdateSavedQueryRequest request) {
    logger.debug("Updating saved query: {}", id);

    try {
      PersistentStore<SavedQuery> store = getStore();
      SavedQuery query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Query not found"))
            .build();
      }

      // Only owner can update
      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can update this query"))
            .build();
      }

      // Update fields if provided
      if (request.name != null) {
        query.setName(request.name.trim());
      }
      if (request.description != null) {
        query.setDescription(request.description);
      }
      if (request.sql != null) {
        query.setSql(request.sql);
      }
      if (request.defaultSchema != null) {
        query.setDefaultSchema(request.defaultSchema);
      }
      if (request.tags != null) {
        query.setTags(request.tags);
      }
      if (request.isPublic != null) {
        query.setPublic(request.isPublic);
      }

      query.setUpdatedAt(Instant.now().toEpochMilli());

      store.put(id, query);

      return Response.ok(query).build();
    } catch (Exception e) {
      logger.error("Error updating saved query", e);
      throw new DrillRuntimeException("Failed to update saved query: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete saved query", description = "Soft-deletes a saved query (recoverable from Trash)")
  public Response deleteSavedQuery(
      @Parameter(description = "Query ID") @PathParam("id") String id) {
    logger.debug("Deleting saved query: {}", id);

    try {
      PersistentStore<SavedQuery> store = getStore();
      SavedQuery query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Query not found"))
            .build();
      }

      // Only owner can delete
      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can delete this query"))
            .build();
      }

      query.setDeletedAt(Instant.now().toEpochMilli());
      store.put(id, query);

      return Response.ok(new MessageResponse("Query moved to trash")).build();
    } catch (Exception e) {
      logger.error("Error deleting saved query", e);
      throw new DrillRuntimeException("Failed to delete saved query: " + e.getMessage(), e);
    }
  }

  @GET
  @Path("/trash")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List trashed queries", description = "Returns soft-deleted queries owned by the current user")
  public SavedQueriesResponse listTrash() {
    List<SavedQuery> queries = new ArrayList<>();
    String currentUser = getCurrentUser();
    long expiryCutoff = Instant.now().toEpochMilli() - TRASH_TTL_MS;

    try {
      PersistentStore<SavedQuery> store = getStore();
      Iterator<Map.Entry<String, SavedQuery>> iterator = store.getAll();

      while (iterator.hasNext()) {
        Map.Entry<String, SavedQuery> entry = iterator.next();
        SavedQuery query = entry.getValue();
        if (query.getDeletedAt() <= 0) {
          continue;
        }
        // Auto-purge items past the 30-day retention window
        if (query.getDeletedAt() < expiryCutoff) {
          try {
            store.delete(entry.getKey());
          } catch (Exception e) {
            logger.warn("Failed to auto-purge expired query {}: {}", entry.getKey(), e.getMessage());
          }
          continue;
        }
        if (query.getOwner().equals(currentUser)) {
          queries.add(query);
        }
      }
    } catch (Exception e) {
      logger.error("Error listing trashed queries", e);
      throw new DrillRuntimeException("Failed to list trashed queries: " + e.getMessage(), e);
    }

    return new SavedQueriesResponse(queries);
  }

  @POST
  @Path("/{id}/restore")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Restore saved query", description = "Restores a soft-deleted query from trash")
  public Response restoreSavedQuery(
      @Parameter(description = "Query ID") @PathParam("id") String id) {
    try {
      PersistentStore<SavedQuery> store = getStore();
      SavedQuery query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Query not found"))
            .build();
      }

      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can restore this query"))
            .build();
      }

      query.setDeletedAt(0L);
      store.put(id, query);
      return Response.ok(query).build();
    } catch (Exception e) {
      logger.error("Error restoring saved query", e);
      throw new DrillRuntimeException("Failed to restore saved query: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}/purge")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Permanently delete saved query", description = "Hard-deletes a query from trash")
  public Response purgeSavedQuery(
      @Parameter(description = "Query ID") @PathParam("id") String id) {
    try {
      PersistentStore<SavedQuery> store = getStore();
      SavedQuery query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Query not found"))
            .build();
      }

      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can purge this query"))
            .build();
      }

      store.delete(id);
      return Response.ok(new MessageResponse("Query permanently deleted")).build();
    } catch (Exception e) {
      logger.error("Error purging saved query", e);
      throw new DrillRuntimeException("Failed to purge saved query: " + e.getMessage(), e);
    }
  }

  // ==================== Helper Methods ====================

  private PersistentStore<SavedQuery> getStore() {
    if (cachedStore == null) {
      synchronized (SavedQueryResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    SavedQuery.class
                )
                .name(STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access saved queries store", e);
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
