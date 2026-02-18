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
import org.apache.drill.exec.server.rest.QueryWrapper.RestQueryBuilder;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.stream.QueryRunner;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.PermitAll;
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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for managing shared query APIs.
 * Allows users to expose query results as public REST endpoints.
 */
@Path("/api/v1/shared-queries")
@Tag(name = "Shared Query APIs", description = "APIs for sharing query results as REST endpoints")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class SharedQueryApiResources {
  private static final Logger logger = LoggerFactory.getLogger(SharedQueryApiResources.class);
  private static final String STORE_NAME = "drill.sqllab.shared_query_apis";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  @Inject
  WebUserConnection webUserConnection;

  private static volatile PersistentStore<SharedQueryApi> cachedStore;

  // ==================== Model Classes ====================

  /**
   * Shared query API model for persistence.
   */
  public static class SharedQueryApi {
    @JsonProperty
    private String id;
    @JsonProperty
    private String name;
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
    private boolean apiEnabled;

    // Default constructor for Jackson
    public SharedQueryApi() {
    }

    @JsonCreator
    public SharedQueryApi(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("sql") String sql,
        @JsonProperty("defaultSchema") String defaultSchema,
        @JsonProperty("owner") String owner,
        @JsonProperty("createdAt") long createdAt,
        @JsonProperty("updatedAt") long updatedAt,
        @JsonProperty("apiEnabled") boolean apiEnabled) {
      this.id = id;
      this.name = name;
      this.sql = sql;
      this.defaultSchema = defaultSchema;
      this.owner = owner;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.apiEnabled = apiEnabled;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
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

    public boolean isApiEnabled() {
      return apiEnabled;
    }

    public void setName(String name) {
      this.name = name;
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

    public void setApiEnabled(boolean apiEnabled) {
      this.apiEnabled = apiEnabled;
    }
  }

  /**
   * Request body for creating a shared query API.
   */
  public static class CreateRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String sql;
    @JsonProperty
    public String defaultSchema;
    @JsonProperty
    public boolean apiEnabled;
  }

  /**
   * Request body for updating a shared query API.
   */
  public static class UpdateRequest {
    @JsonProperty
    public String name;
    @JsonProperty
    public String sql;
    @JsonProperty
    public String defaultSchema;
    @JsonProperty
    public Boolean apiEnabled;
  }

  /**
   * Response containing a list of shared query APIs.
   */
  public static class SharedQueryApisResponse {
    @JsonProperty
    public List<SharedQueryApi> queries;

    public SharedQueryApisResponse(List<SharedQueryApi> queries) {
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

  // ==================== CRUD Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List shared query APIs", description = "Returns all shared query APIs owned by the current user")
  public SharedQueryApisResponse listSharedQueryApis() {
    logger.debug("Listing shared query APIs for user: {}", getCurrentUser());

    List<SharedQueryApi> queries = new ArrayList<>();
    String currentUser = getCurrentUser();

    try {
      PersistentStore<SharedQueryApi> store = getStore();
      Iterator<Map.Entry<String, SharedQueryApi>> iterator = store.getAll();

      while (iterator.hasNext()) {
        Map.Entry<String, SharedQueryApi> entry = iterator.next();
        SharedQueryApi query = entry.getValue();

        if (query.getOwner().equals(currentUser)) {
          queries.add(query);
        }
      }
    } catch (Exception e) {
      logger.error("Error listing shared query APIs", e);
      throw new DrillRuntimeException("Failed to list shared query APIs: " + e.getMessage(), e);
    }

    return new SharedQueryApisResponse(queries);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create shared query API", description = "Creates a new shared query API endpoint")
  public Response createSharedQueryApi(CreateRequest request) {
    logger.debug("Creating shared query API: {}", request.name);

    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("SQL is required"))
          .build();
    }

    String id = UUID.randomUUID().toString();
    long now = Instant.now().toEpochMilli();

    SharedQueryApi query = new SharedQueryApi(
        id,
        request.name != null ? request.name.trim() : "Shared Query API",
        request.sql,
        request.defaultSchema,
        getCurrentUser(),
        now,
        now,
        request.apiEnabled
    );

    try {
      PersistentStore<SharedQueryApi> store = getStore();
      store.put(id, query);
    } catch (Exception e) {
      logger.error("Error creating shared query API", e);
      throw new DrillRuntimeException("Failed to create shared query API: " + e.getMessage(), e);
    }

    return Response.status(Response.Status.CREATED).entity(query).build();
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get shared query API", description = "Returns a shared query API by ID")
  public Response getSharedQueryApi(
      @Parameter(description = "Shared query API ID") @PathParam("id") String id) {
    logger.debug("Getting shared query API: {}", id);

    try {
      PersistentStore<SharedQueryApi> store = getStore();
      SharedQueryApi query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Shared query API not found"))
            .build();
      }

      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Access denied"))
            .build();
      }

      return Response.ok(query).build();
    } catch (Exception e) {
      logger.error("Error getting shared query API", e);
      throw new DrillRuntimeException("Failed to get shared query API: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update shared query API", description = "Updates an existing shared query API")
  public Response updateSharedQueryApi(
      @Parameter(description = "Shared query API ID") @PathParam("id") String id,
      UpdateRequest request) {
    logger.debug("Updating shared query API: {}", id);

    try {
      PersistentStore<SharedQueryApi> store = getStore();
      SharedQueryApi query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Shared query API not found"))
            .build();
      }

      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can update this shared query API"))
            .build();
      }

      if (request.name != null) {
        query.setName(request.name.trim());
      }
      if (request.sql != null) {
        query.setSql(request.sql);
      }
      if (request.defaultSchema != null) {
        query.setDefaultSchema(request.defaultSchema);
      }
      if (request.apiEnabled != null) {
        query.setApiEnabled(request.apiEnabled);
      }

      query.setUpdatedAt(Instant.now().toEpochMilli());

      store.put(id, query);

      return Response.ok(query).build();
    } catch (Exception e) {
      logger.error("Error updating shared query API", e);
      throw new DrillRuntimeException("Failed to update shared query API: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete shared query API", description = "Deletes a shared query API")
  public Response deleteSharedQueryApi(
      @Parameter(description = "Shared query API ID") @PathParam("id") String id) {
    logger.debug("Deleting shared query API: {}", id);

    try {
      PersistentStore<SharedQueryApi> store = getStore();
      SharedQueryApi query = store.get(id);

      if (query == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Shared query API not found"))
            .build();
      }

      if (!query.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can delete this shared query API"))
            .build();
      }

      store.delete(id);

      return Response.ok(new MessageResponse("Shared query API deleted successfully")).build();
    } catch (Exception e) {
      logger.error("Error deleting shared query API", e);
      throw new DrillRuntimeException("Failed to delete shared query API: " + e.getMessage(), e);
    }
  }

  // ==================== Public Data Endpoint ====================

  @GET
  @Path("/{id}/data")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @Operation(summary = "Get shared query data", description = "Executes the shared query and returns results as JSON")
  public Response getSharedQueryData(
      @Parameter(description = "Shared query API ID") @PathParam("id") String id) {
    logger.debug("Fetching data for shared query API: {}", id);

    SharedQueryApi query;
    try {
      PersistentStore<SharedQueryApi> store = getStore();
      query = store.get(id);
    } catch (Exception e) {
      logger.error("Error accessing shared query API store", e);
      throw new DrillRuntimeException("Failed to access shared query API: " + e.getMessage(), e);
    }

    if (query == null || !query.isApiEnabled()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(new MessageResponse("Shared query API not found"))
          .build();
    }

    QueryWrapper queryWrapper = new RestQueryBuilder()
        .query(query.getSql())
        .queryType("SQL")
        .rowLimit(10000)
        .defaultSchema(query.getDefaultSchema())
        .build();

    QueryRunner runner = new QueryRunner(workManager, webUserConnection);
    try {
      runner.start(queryWrapper);
    } catch (Exception e) {
      throw new WebApplicationException("Query execution failed", e);
    }

    StreamingOutput streamingOutput = new StreamingOutput() {
      @Override
      public void write(OutputStream output)
          throws IOException, WebApplicationException {
        try {
          runner.sendResults(output);
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new WebApplicationException("Query execution failed", e);
        }
      }
    };

    return Response.ok(streamingOutput)
        .header("Access-Control-Allow-Origin", "*")
        .build();
  }

  // ==================== Helper Methods ====================

  private PersistentStore<SharedQueryApi> getStore() {
    if (cachedStore == null) {
      synchronized (SharedQueryApiResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    SharedQueryApi.class
                )
                .name(STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access shared query APIs store", e);
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
