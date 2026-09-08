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
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST resource for notebook data export.
 * Allows writing DataFrames from the browser-side notebook back to Drill storage workspaces.
 *
 * <p>Security:
 * <ul>
 *   <li>All endpoints require authentication ({@code AUTHENTICATED_ROLE})</li>
 *   <li>The export endpoint enforces the {@code web.notebook.write_policy} system option:
 *       {@code "filesystem"} (default) allows any authenticated user,
 *       {@code "admin_only"} restricts to admin users,
 *       {@code "disabled"} blocks all exports</li>
 *   <li>CSRF protection via {@code X-CSRF-Token} header validation on POST</li>
 *   <li>Path traversal protection via table name sanitization and resolved path validation</li>
 * </ul>
 */
@Path("/api/v1/notebook")
@Tag(name = "Notebook", description = "Notebook data export for Apache Drill SQL Lab")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class NotebookResources {

  private static final Logger logger = LoggerFactory.getLogger(NotebookResources.class);
  private static final long MAX_EXPORT_DATA_BYTES = 100 * 1024 * 1024; // 100 MB limit

  @Inject
  WorkManager workManager;

  @Context
  SecurityContext sc;

  @Context
  HttpServletRequest request;

  // ==================== Request/Response Models ====================

  public static class ExportRequest {
    @JsonProperty
    public String plugin;

    @JsonProperty
    public String workspace;

    @JsonProperty
    public String tableName;

    @JsonProperty
    public String format;

    @JsonProperty
    public String data;

    public ExportRequest() {
    }

    @JsonCreator
    public ExportRequest(
        @JsonProperty("plugin") String plugin,
        @JsonProperty("workspace") String workspace,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("format") String format,
        @JsonProperty("data") String data) {
      this.plugin = plugin;
      this.workspace = workspace;
      this.tableName = tableName;
      this.format = format;
      this.data = data;
    }
  }

  public static class ExportResponse {
    @JsonProperty
    public boolean success;

    @JsonProperty
    public String message;

    @JsonProperty
    public String path;

    public ExportResponse(boolean success, String message, String path) {
      this.success = success;
      this.message = message;
      this.path = path;
    }
  }

  public static class WorkspaceInfo {
    @JsonProperty
    public String plugin;

    @JsonProperty
    public String workspace;

    @JsonProperty
    public String location;

    @JsonProperty
    public boolean writable;

    public WorkspaceInfo(String plugin, String workspace, String location, boolean writable) {
      this.plugin = plugin;
      this.workspace = workspace;
      this.location = location;
      this.writable = writable;
    }
  }

  public static class WorkspacesResponse {
    @JsonProperty
    public List<WorkspaceInfo> workspaces;

    @JsonProperty
    public String writePolicy;

    public WorkspacesResponse(List<WorkspaceInfo> workspaces, String writePolicy) {
      this.workspaces = workspaces;
      this.writePolicy = writePolicy;
    }
  }

  public static class ErrorResponse {
    @JsonProperty
    public String message;

    public ErrorResponse(String message) {
      this.message = message;
    }
  }

  // ==================== Endpoints ====================

  @GET
  @Path("/workspaces")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List writable workspaces",
      description = "Returns all writable file system workspaces where data can be exported")
  public Response listWorkspaces() {
    try {
      String writePolicy = getWritePolicy();

      // If exports are disabled, return empty list with the policy
      if ("disabled".equals(writePolicy)) {
        return Response.ok(new WorkspacesResponse(new ArrayList<>(), writePolicy)).build();
      }

      // If admin_only, check role and return empty for non-admins
      if ("admin_only".equals(writePolicy) && !isAdmin()) {
        return Response.ok(new WorkspacesResponse(new ArrayList<>(), writePolicy)).build();
      }

      StoragePluginRegistry registry = workManager.getContext().getStorage();
      List<WorkspaceInfo> result = new ArrayList<>();

      for (Map.Entry<String, StoragePlugin> entry : registry) {
        String pluginName = entry.getKey();
        StoragePlugin plugin = entry.getValue();
        StoragePluginConfig config = plugin.getConfig();

        if (config instanceof FileSystemConfig) {
          FileSystemConfig fsConfig = (FileSystemConfig) config;
          Map<String, WorkspaceConfig> workspaces = fsConfig.getWorkspaces();

          if (workspaces != null) {
            for (Map.Entry<String, WorkspaceConfig> wsEntry : workspaces.entrySet()) {
              WorkspaceConfig wsConfig = wsEntry.getValue();
              if (wsConfig.isWritable()) {
                result.add(new WorkspaceInfo(
                    pluginName,
                    wsEntry.getKey(),
                    wsConfig.getLocation(),
                    true
                ));
              }
            }
          }
        }
      }

      return Response.ok(new WorkspacesResponse(result, writePolicy)).build();
    } catch (Exception e) {
      logger.error("Error listing workspaces", e);
      return Response.serverError()
          .entity(new ErrorResponse("Failed to list workspaces: " + e.getMessage()))
          .build();
    }
  }

  @POST
  @Path("/export")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Export notebook data to a Drill workspace",
      description = "Writes serialized DataFrame data (JSON or CSV) to a file in a Drill workspace")
  public Response exportData(ExportRequest request,
      @HeaderParam("X-CSRF-Token") String csrfHeader) {

    // ─── CSRF validation ─────────────────────────────────────────
    if (!validateCsrfToken(csrfHeader)) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new ErrorResponse("CSRF token validation failed"))
          .build();
    }

    // ─── Write policy check ──────────────────────────────────────
    String writePolicy = getWritePolicy();
    if ("disabled".equals(writePolicy)) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new ErrorResponse("Notebook data export is disabled by the administrator. "
              + "The system option 'web.notebook.write_policy' is set to 'disabled'."))
          .build();
    }
    if ("admin_only".equals(writePolicy) && !isAdmin()) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new ErrorResponse("Only administrators can export notebook data. "
              + "The system option 'web.notebook.write_policy' is set to 'admin_only'."))
          .build();
    }

    // ─── Input validation ────────────────────────────────────────
    if (request.plugin == null || request.plugin.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Missing 'plugin' field"))
          .build();
    }
    if (request.workspace == null || request.workspace.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Missing 'workspace' field"))
          .build();
    }
    if (request.tableName == null || request.tableName.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Missing 'tableName' field"))
          .build();
    }
    if (request.data == null || request.data.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Missing 'data' field"))
          .build();
    }

    // ─── Size limit check ────────────────────────────────────────
    if (request.data.length() > MAX_EXPORT_DATA_BYTES) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Data exceeds maximum allowed size of "
              + (MAX_EXPORT_DATA_BYTES / (1024 * 1024)) + " MB"))
          .build();
    }

    String format = request.format != null ? request.format.toLowerCase() : "json";
    if (!"json".equals(format) && !"csv".equals(format)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Unsupported format: " + format + ". Use 'json' or 'csv'."))
          .build();
    }

    // Sanitize table name to prevent path traversal
    String safeName = request.tableName.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    if (safeName.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new ErrorResponse("Invalid table name"))
          .build();
    }

    try {
      StoragePluginRegistry registry = workManager.getContext().getStorage();
      StoragePlugin plugin = registry.getPlugin(request.plugin);

      if (plugin == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new ErrorResponse("Storage plugin not found: " + request.plugin))
            .build();
      }

      if (!(plugin instanceof FileSystemPlugin)) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Plugin '" + request.plugin + "' is not a file system plugin"))
            .build();
      }

      FileSystemPlugin fsPlugin = (FileSystemPlugin) plugin;
      FileSystemConfig fsConfig = (FileSystemConfig) fsPlugin.getConfig();
      Map<String, WorkspaceConfig> workspaces = fsConfig.getWorkspaces();

      if (workspaces == null || !workspaces.containsKey(request.workspace)) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new ErrorResponse("Workspace not found: " + request.workspace))
            .build();
      }

      WorkspaceConfig wsConfig = workspaces.get(request.workspace);
      if (!wsConfig.isWritable()) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new ErrorResponse("Workspace '" + request.workspace + "' is not writable"))
            .build();
      }

      // Resolve the workspace location
      String wsLocation = wsConfig.getLocation();
      String connection = fsConfig.getConnection();
      java.nio.file.Path basePath;
      if (connection != null && connection.startsWith("file:///")) {
        basePath = Paths.get(connection.substring(7));
      } else if (connection != null && connection.startsWith("file:/")) {
        basePath = Paths.get(connection.substring(5));
      } else {
        basePath = Paths.get("/");
      }

      java.nio.file.Path fullPath = basePath.resolve(wsLocation).resolve(safeName + "." + format);

      // Ensure the resolved path is still within the workspace directory
      java.nio.file.Path wsDir = basePath.resolve(wsLocation).normalize();
      java.nio.file.Path resolvedFile = fullPath.normalize();
      if (!resolvedFile.startsWith(wsDir)) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Invalid table name: path traversal detected"))
            .build();
      }

      // Create parent directories if needed
      File parentDir = resolvedFile.getParent().toFile();
      if (!parentDir.exists()) {
        if (!parentDir.mkdirs()) {
          return Response.serverError()
              .entity(new ErrorResponse("Failed to create directory: " + parentDir))
              .build();
        }
      }

      // Write the file
      Files.write(resolvedFile, request.data.getBytes(StandardCharsets.UTF_8));

      String queryPath = request.plugin + "." + request.workspace + ".`" + safeName + "." + format + "`";
      String userName = sc.getUserPrincipal() != null ? sc.getUserPrincipal().getName() : "anonymous";
      logger.info("User '{}' exported notebook data to {} (policy: {})", userName, resolvedFile, writePolicy);

      return Response.ok(new ExportResponse(
          true,
          "Data exported successfully",
          queryPath
      )).build();

    } catch (IOException e) {
      logger.error("Error writing export file", e);
      return Response.serverError()
          .entity(new ErrorResponse("Failed to write file: " + e.getMessage()))
          .build();
    } catch (Exception e) {
      logger.error("Error during data export", e);
      return Response.serverError()
          .entity(new ErrorResponse("Export failed: " + e.getMessage()))
          .build();
    }
  }

  // ==================== Helper Methods ====================

  private String getWritePolicy() {
    try {
      return workManager.getContext().getOptionManager()
          .getString(ExecConstants.NOTEBOOK_WRITE_POLICY);
    } catch (Exception e) {
      logger.warn("Failed to read notebook write policy, defaulting to 'filesystem'", e);
      return "filesystem";
    }
  }

  private boolean isAdmin() {
    return sc != null && sc.isUserInRole(DrillUserPrincipal.ADMIN_ROLE);
  }

  private boolean validateCsrfToken(String csrfHeader) {
    // Get the CSRF token from the session
    String sessionToken = WebUtils.getCsrfTokenFromHttpRequest(request);

    // If no session token exists (auth disabled or no session), allow the request
    if (sessionToken == null || sessionToken.isEmpty()) {
      return true;
    }

    // Validate the header token against the session token
    return sessionToken.equals(csrfHeader);
  }
}
