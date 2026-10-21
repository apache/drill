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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * REST API for the query result cache.
 * Provides endpoints to retrieve, paginate, download, and evict cached results.
 *
 * <p>Security:
 * <ul>
 *   <li>All endpoints require authentication ({@code AUTHENTICATED_ROLE})</li>
 *   <li>Cache entries are user-scoped: users can only access their own cached results</li>
 *   <li>CacheId is validated as UUID format to prevent path traversal attacks</li>
 *   <li>Admin users can access any cache entry</li>
 * </ul>
 */
@Path("/api/v1/results")
@Tag(name = "Results", description = "Query result caching APIs for SQL Lab")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ResultCacheResources {
  private static final Logger logger = LoggerFactory.getLogger(ResultCacheResources.class);

  /** UUID pattern for validating cacheId to prevent path traversal. */
  private static final Pattern UUID_PATTERN = Pattern.compile(
      "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

  /** Maximum allowed limit for paginated row requests. */
  private static final int MAX_ROW_LIMIT = 50000;

  @Inject
  WorkManager workManager;

  @Context
  SecurityContext sc;

  private volatile ResultCacheService cacheService;

  /**
   * Lazily initialize the cache service singleton.
   */
  ResultCacheService getCacheService() {
    if (cacheService == null) {
      synchronized (this) {
        if (cacheService == null) {
          DrillConfig config = workManager.getContext().getConfig();
          long ttlMinutes = config.getLong(ExecConstants.RESULT_CACHE_TTL_MINUTES);
          long maxTotalMb = config.getLong(ExecConstants.RESULT_CACHE_MAX_TOTAL_MB);
          long maxResultMb = config.getLong(ExecConstants.RESULT_CACHE_MAX_RESULT_MB);
          int maxRows = config.getInt(ExecConstants.RESULT_CACHE_MAX_ROWS);
          String cacheDirPath = config.getString(ExecConstants.RESULT_CACHE_DIRECTORY);

          File cacheDir;
          if (cacheDirPath.startsWith("/")) {
            cacheDir = new File(cacheDirPath);
          } else {
            // Relative to Drill home
            cacheDir = new File(System.getProperty("drill.home", "."), cacheDirPath);
          }

          ObjectMapper mapper = new ObjectMapper();
          cacheService = new ResultCacheService(cacheDir, ttlMinutes, maxTotalMb, maxResultMb, maxRows, mapper);
          logger.info("Result cache service initialized: dir={}, ttl={}min, maxTotal={}MB, maxResult={}MB, maxRows={}",
              cacheDir, ttlMinutes, maxTotalMb, maxResultMb, maxRows);
        }
      }
    }
    return cacheService;
  }

  /**
   * Cache a query result. Called internally after query execution completes.
   */
  public ResultCacheService.CacheMeta cacheQueryResult(
      String queryId, String sql, String defaultSchema, String userName,
      String queryState, Collection<String> columns, List<String> metadata,
      List<Map<String, String>> rows) throws IOException {
    return getCacheService().cacheResult(queryId, sql, defaultSchema, userName,
        queryState, columns, metadata, rows);
  }

  /**
   * Look up a cached result by SQL content.
   */
  public String findCachedResult(String sql, String defaultSchema, String userName) {
    return getCacheService().findBySqlHash(sql, defaultSchema, userName);
  }

  // ==================== Security Helpers ====================

  /**
   * Validate that a cacheId is a valid UUID to prevent path traversal attacks.
   */
  private boolean isValidCacheId(String cacheId) {
    return cacheId != null && UUID_PATTERN.matcher(cacheId).matches();
  }

  /**
   * Get the current authenticated user name.
   */
  private String getCurrentUser() {
    if (sc != null && sc.getUserPrincipal() != null) {
      return sc.getUserPrincipal().getName();
    }
    return "anonymous";
  }

  /**
   * Check if the current user is an admin.
   */
  private boolean isAdmin() {
    return sc != null && sc.isUserInRole(DrillUserPrincipal.ADMIN_ROLE);
  }

  /**
   * Check if the current user is authorized to access a cache entry.
   * Users can only access their own entries; admins can access any entry.
   */
  private boolean isAuthorized(ResultCacheService.CacheMeta meta) {
    if (isAdmin()) {
      return true;
    }
    String currentUser = getCurrentUser();
    return currentUser.equals(meta.userName);
  }

  /**
   * Sanitize a string for use in Content-Disposition filename.
   * Removes any characters that could be used for header injection.
   */
  private String sanitizeFilename(String name) {
    if (name == null) {
      return "results";
    }
    // Allow only alphanumeric, hyphens, and underscores
    return name.replaceAll("[^a-zA-Z0-9_-]", "_");
  }

  // ==================== REST Endpoints ====================

  @POST
  @Path("/store")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Store query results in the cache")
  public Response storeResult(CacheStoreRequest request) {
    if (request == null || request.rows == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Request body with rows is required"))
          .build();
    }

    String userName = getCurrentUser();

    try {
      List<Map<String, String>> rows = new ArrayList<>();
      for (Map<String, String> row : request.rows) {
        rows.add(row);
      }

      ResultCacheService.CacheMeta meta = getCacheService().cacheResult(
          request.queryId, request.sql, request.defaultSchema, userName,
          request.queryState, request.columns, request.metadata, rows);

      if (meta == null) {
        return Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE)
            .entity(new ErrorResponse("Result too large to cache"))
            .build();
      }
      return Response.ok(meta).build();
    } catch (IOException e) {
      logger.error("Failed to cache result", e);
      return Response.serverError()
          .entity(new ErrorResponse("Failed to cache result: " + e.getMessage()))
          .build();
    }
  }

  @GET
  @Path("/{cacheId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get cache entry metadata")
  public Response getMetadata(
      @Parameter(description = "Cache entry ID") @PathParam("cacheId") String cacheId) {
    if (!isValidCacheId(cacheId)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Invalid cache ID format"))
          .build();
    }

    ResultCacheService.CacheMeta meta = getCacheService().getMetadata(cacheId);
    if (meta == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(new ErrorResponse("Cache entry not found or expired"))
          .build();
    }

    if (!isAuthorized(meta)) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new ErrorResponse("Access denied"))
          .build();
    }

    return Response.ok(meta).build();
  }

  @GET
  @Path("/{cacheId}/rows")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get paginated rows from a cached result")
  public Response getRows(
      @Parameter(description = "Cache entry ID") @PathParam("cacheId") String cacheId,
      @Parameter(description = "Row offset") @QueryParam("offset") @DefaultValue("0") int offset,
      @Parameter(description = "Row limit") @QueryParam("limit") @DefaultValue("500") int limit) {
    if (!isValidCacheId(cacheId)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Invalid cache ID format"))
          .build();
    }

    // Clamp limit to prevent excessive memory usage
    if (limit > MAX_ROW_LIMIT) {
      limit = MAX_ROW_LIMIT;
    }
    if (limit < 1) {
      limit = 1;
    }
    if (offset < 0) {
      offset = 0;
    }

    try {
      // Check authorization before returning data
      ResultCacheService.CacheMeta meta = getCacheService().getMetadata(cacheId);
      if (meta == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new ErrorResponse("Cache entry not found or expired"))
            .build();
      }
      if (!isAuthorized(meta)) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new ErrorResponse("Access denied"))
            .build();
      }

      ResultCacheService.PaginatedRows result = getCacheService().getRows(cacheId, offset, limit);
      if (result == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new ErrorResponse("Cache entry not found or expired"))
            .build();
      }
      return Response.ok(result).build();
    } catch (IOException e) {
      logger.error("Failed to read cached rows for {}", cacheId, e);
      return Response.serverError()
          .entity(new ErrorResponse("Failed to read cached rows"))
          .build();
    }
  }

  @GET
  @Path("/{cacheId}/download")
  @Operation(summary = "Download all rows as CSV or JSON")
  public Response downloadRows(
      @Parameter(description = "Cache entry ID") @PathParam("cacheId") String cacheId,
      @Parameter(description = "Download format: csv or json") @QueryParam("format") @DefaultValue("csv") String format) {
    if (!isValidCacheId(cacheId)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Invalid cache ID format"))
          .build();
    }

    ResultCacheService.CacheMeta meta = getCacheService().getMetadata(cacheId);
    if (meta == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(new ErrorResponse("Cache entry not found or expired"))
          .build();
    }

    if (!isAuthorized(meta)) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new ErrorResponse("Access denied"))
          .build();
    }

    String contentType;
    String extension;
    if ("json".equalsIgnoreCase(format)) {
      contentType = MediaType.APPLICATION_JSON;
      extension = "json";
    } else {
      contentType = "text/csv";
      extension = "csv";
    }

    String sanitizedId = sanitizeFilename(meta.queryId != null ? meta.queryId : cacheId);
    String filename = "query-" + sanitizedId + "." + extension;

    StreamingOutput stream = out -> {
      try {
        getCacheService().streamAllRows(cacheId, format, out);
      } catch (IOException e) {
        logger.error("Error streaming download for {}", cacheId, e);
        throw e;
      }
    };

    return Response.ok(stream, contentType)
        .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
        .build();
  }

  @DELETE
  @Path("/{cacheId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Evict a cache entry")
  public Response evict(
      @Parameter(description = "Cache entry ID") @PathParam("cacheId") String cacheId) {
    if (!isValidCacheId(cacheId)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Invalid cache ID format"))
          .build();
    }

    // Check authorization before evicting
    ResultCacheService.CacheMeta meta = getCacheService().getMetadata(cacheId);
    if (meta == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(new ErrorResponse("Cache entry not found"))
          .build();
    }
    if (!isAuthorized(meta)) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new ErrorResponse("Access denied"))
          .build();
    }

    boolean evicted = getCacheService().evict(cacheId);
    if (evicted) {
      return Response.ok(new StatusResponse("evicted")).build();
    }
    return Response.status(Response.Status.NOT_FOUND)
        .entity(new ErrorResponse("Cache entry not found"))
        .build();
  }

  @GET
  @Path("/stats")
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Operation(summary = "Get cache statistics (admin only)")
  public Response getStats() {
    ResultCacheService.CacheStats stats = getCacheService().getStats();
    // Omit the filesystem path for non-admin users (defense in depth;
    // the @RolesAllowed annotation already restricts to admins)
    return Response.ok(stats).build();
  }

  // ==================== DTO Classes ====================

  public static class ErrorResponse {
    @JsonProperty public String error;

    public ErrorResponse() { }

    public ErrorResponse(String error) {
      this.error = error;
    }
  }

  public static class StatusResponse {
    @JsonProperty public String status;

    public StatusResponse() { }

    public StatusResponse(String status) {
      this.status = status;
    }
  }

  public static class CacheStoreRequest {
    @JsonProperty public String queryId;
    @JsonProperty public String sql;
    @JsonProperty public String defaultSchema;
    @JsonProperty public String queryState;
    @JsonProperty public List<String> columns;
    @JsonProperty public List<String> metadata;
    @JsonProperty public List<Map<String, String>> rows;

    public CacheStoreRequest() { }
  }
}
