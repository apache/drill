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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;

import java.util.Map;

/**
 * The cluster-wide upload target: the one writable storage plugin workspace that
 * project file uploads are written under. Admins set it; any authenticated user can
 * read whether it is configured and where it points.
 */
@Path("/api/v1/uploads")
@Tag(name = "Uploads", description = "Cluster-wide target for project file uploads")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class UploadConfigResources {

  private static final String STORE_NAME = "drill.sqllab.upload_config";
  private static final String CONFIG_KEY = "default";
  static final long DEFAULT_MAX_SIZE_BYTES = 1_073_741_824L;

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class UploadConfig {
    @JsonProperty
    public String plugin;

    @JsonProperty
    public String workspace;

    @JsonProperty
    public long maxSizeBytes = DEFAULT_MAX_SIZE_BYTES;

    public UploadConfig() {
    }

    @JsonCreator
    public UploadConfig(@JsonProperty("plugin") String plugin,
        @JsonProperty("workspace") String workspace,
        @JsonProperty("maxSizeBytes") Long maxSizeBytes) {
      this.plugin = plugin;
      this.workspace = workspace;
      this.maxSizeBytes = maxSizeBytes == null || maxSizeBytes <= 0
          ? DEFAULT_MAX_SIZE_BYTES : maxSizeBytes;
    }
  }

  public static class StatusResponse {
    @JsonProperty
    public boolean configured;

    @JsonProperty
    public String plugin;

    @JsonProperty
    public String workspace;

    @JsonProperty
    public long maxSizeBytes;
  }

  public static class SaveResponse {
    @JsonProperty
    public UploadConfig config;

    @JsonProperty
    public String warning;
  }

  public static class ErrorResponse {
    @JsonProperty
    public String message;

    public ErrorResponse(String message) {
      this.message = message;
    }
  }

  /** Returns the stored upload target, or null when none has been set. */
  public static UploadConfig current(PersistentStoreProvider provider) {
    return openStore(provider).get(CONFIG_KEY);
  }

  /** Removes the stored target. Test support. */
  static void clearForTest(PersistentStoreProvider provider) {
    openStore(provider).delete(CONFIG_KEY);
  }

  // Some providers hand back a fresh store per call, so keep one per provider.
  private static PersistentStoreProvider cachedFor;
  private static PersistentStore<UploadConfig> cachedStore;

  private static synchronized PersistentStore<UploadConfig> openStore(
      PersistentStoreProvider provider) {
    if (cachedStore == null || cachedFor != provider) {
      try {
        cachedStore = provider.getOrCreateStore(
            PersistentStoreConfig.newJacksonBuilder(new ObjectMapper(), UploadConfig.class)
                .name(STORE_NAME)
                .build());
        cachedFor = provider;
      } catch (StoreException e) {
        throw new DrillRuntimeException("Failed to access upload config store", e);
      }
    }
    return cachedStore;
  }

  /**
   * Advisory text for a target that only one node can see, or null. A local
   * (file:) connection on a multi-node cluster would scatter uploads across nodes.
   */
  static String multiNodeWarning(String connection, int drillbitCount) {
    if (connection != null && connection.startsWith("file:") && drillbitCount > 1) {
      return "This workspace uses a local file system and the cluster has " + drillbitCount
          + " Drillbits. Uploaded files will only exist on the node that receives them. "
          + "Use shared storage unless this is intended.";
    }
    return null;
  }

  @GET
  @Path("/config")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get the upload target", description = "Admin only")
  public Response getConfig() {
    UploadConfig config = current(storeProvider);
    return Response.ok(config == null ? new UploadConfig() : config).build();
  }

  @GET
  @Path("/status")
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Upload status", description = "Whether uploads are configured and where they go")
  public Response getStatus() {
    UploadConfig config = current(storeProvider);
    StatusResponse status = new StatusResponse();
    if (config != null && config.plugin != null && config.workspace != null) {
      status.configured = true;
      status.plugin = config.plugin;
      status.workspace = config.workspace;
      status.maxSizeBytes = config.maxSizeBytes;
    }
    return Response.ok(status).build();
  }

  @PUT
  @Path("/config")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Set the upload target", description = "Admin only")
  public Response putConfig(UploadConfig request) {
    if (request == null || isBlank(request.plugin) || isBlank(request.workspace)) {
      return error(Response.Status.BAD_REQUEST, "Both plugin and workspace are required");
    }
    StoragePluginRegistry registry = workManager.getContext().getStorage();
    StoragePlugin plugin;
    try {
      plugin = registry.getPlugin(request.plugin);
    } catch (Exception e) {
      return error(Response.Status.INTERNAL_SERVER_ERROR,
          "Could not load storage plugin '" + request.plugin + "': " + e.getMessage());
    }
    if (plugin == null) {
      return error(Response.Status.NOT_FOUND, "Storage plugin not found: " + request.plugin);
    }
    if (!(plugin instanceof FileSystemPlugin)) {
      return error(Response.Status.BAD_REQUEST,
          "Plugin '" + request.plugin + "' is not a file system plugin");
    }
    FileSystemConfig fsConfig = (FileSystemConfig) ((FileSystemPlugin) plugin).getConfig();
    Map<String, WorkspaceConfig> workspaces = fsConfig.getWorkspaces();
    if (workspaces == null || !workspaces.containsKey(request.workspace)) {
      return error(Response.Status.NOT_FOUND, "Workspace not found: " + request.workspace);
    }
    if (!workspaces.get(request.workspace).isWritable()) {
      return error(Response.Status.FORBIDDEN,
          "Workspace '" + request.workspace + "' is not writable");
    }

    UploadConfig saved = new UploadConfig(request.plugin, request.workspace, request.maxSizeBytes);
    openStore(storeProvider).put(CONFIG_KEY, saved);

    SaveResponse response = new SaveResponse();
    response.config = saved;
    response.warning = multiNodeWarning(fsConfig.getConnection(),
        workManager.getContext().getBits().size());
    return Response.ok(response).build();
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }

  private static Response error(Response.Status status, String message) {
    return Response.status(status).entity(new ErrorResponse(message)).build();
  }
}
