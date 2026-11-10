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
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.NameBinding;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.ProjectResources.DatasetRef;
import org.apache.drill.exec.server.rest.ProjectResources.MessageResponse;
import org.apache.drill.exec.server.rest.ProjectResources.Project;
import org.apache.drill.exec.server.rest.UploadConfigResources.UploadConfig;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.WorkManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.nio.charset.StandardCharsets;
import java.lang.annotation.RetentionPolicy;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Files uploaded into a project. Every project gets one folder, named after the project
 * id, inside the cluster's single upload target (see {@link UploadConfigResources}).
 * Drill then reads that folder as a table, so the project gains one {@code folder}
 * dataset reference pointing at it on the first upload.
 *
 * <p>Nothing here looks at the file's extension: Drill decides at query time what it can
 * read, and a file it cannot read is a query error rather than a failed upload. Nothing
 * here branches on the storage connection either, so a local {@code dfs} workspace and an
 * {@code s3a://} one take the same code path. That is why writes go through
 * {@link FileSystem} and never {@code java.nio.file}, which only serves {@code file://}.
 */
@jakarta.ws.rs.Path("/api/v1/projects/{id}/files")
@Tag(name = "Projects", description = "APIs for managing data analytics projects")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
@ProjectFileResources.BoundedUpload
public class ProjectFileResources {

  private static final Logger logger = LoggerFactory.getLogger(ProjectFileResources.class);

  private static final String FOLDER_DATASET_TYPE = "folder";
  /** Headroom for the multipart boundaries and part headers around the file itself. */
  private static final long MULTIPART_ENVELOPE_BYTES = 8192;

  /** {@code .upload-} plus a UUID, the suffix a staged write adds to the name. */
  private static final String STAGING_SUFFIX = ".upload-";
  private static final int STAGING_SUFFIX_BYTES = STAGING_SUFFIX.length() + 36;
  /**
   * The longest name accepted, leaving room for the staging suffix inside the 255 bytes
   * that file systems commonly allow a single path component. Without this a name near
   * the limit would pass here and then fail at create time as a 500.
   */
  private static final int MAX_NAME_BYTES = 255 - STAGING_SUFFIX_BYTES;
  private static final String FOLDER_DATASET_LABEL = "Uploaded files";

  /** Shown whenever the cluster has no usable upload target. */
  private static final String NOT_CONFIGURED =
      "No upload folder is configured for this cluster. "
          + "An administrator must set one before files can be uploaded.";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  @Inject
  StoragePluginRegistry storageRegistry;

  /** The 201 body: where the uploaded file can be queried. */
  public static class UploadResponse {
    @JsonProperty
    public final String name;

    @JsonProperty
    public final String path;

    @JsonProperty
    public final String schema;

    public UploadResponse(String name, String path, String schema) {
      this.name = name;
      this.path = path;
      this.schema = schema;
    }
  }

  /** Binds {@link UploadSizeFilter} to this resource and nothing else. */
  @NameBinding
  @Retention(RetentionPolicy.RUNTIME)
  public @interface BoundedUpload {
  }

  /**
   * Refuses an over-large request from its {@code Content-Length} before the entity is
   * read. Without this the cap would bound only what is kept: Jersey spools any part
   * over a few kilobytes to the Drillbit's temp directory to satisfy the
   * {@code @FormDataParam} injection, which happens after request filters but before the
   * handler runs. A request that declares no length still spools, so this narrows the
   * exposure rather than closing it.
   */
  @Provider
  @BoundedUpload
  public static class UploadSizeFilter implements ContainerRequestFilter {

    @Inject
    PersistentStoreProvider storeProvider;

    @Override
    public void filter(ContainerRequestContext request) {
      long declared = request.getLength();
      if (declared < 0) {
        return;
      }
      UploadConfig cfg = UploadConfigResources.current(storeProvider);
      if (cfg == null) {
        return;
      }
      if (declared > allowedRequestBytes(cfg.maxSizeBytes)) {
        request.abortWith(error(Response.Status.REQUEST_ENTITY_TOO_LARGE,
            "File exceeds the maximum upload size of " + cfg.maxSizeBytes + " bytes"));
      }
    }
  }

  /**
   * The largest whole request a part of {@code maxSizeBytes} can arrive in. The request
   * carries the multipart boundaries and part headers as well as the file, so comparing
   * the raw {@code Content-Length} against the cap would reject a part that is exactly at
   * it. Saturates rather than overflowing.
   */
  static long allowedRequestBytes(long maxSizeBytes) {
    long allowed = maxSizeBytes + MULTIPART_ENVELOPE_BYTES;
    return allowed < maxSizeBytes ? Long.MAX_VALUE : allowed;
  }

  // ==================== Shared helpers (also used by delete and download) ====================

  /**
   * The upload name a client asked for, or null when it is not a plain file name that
   * fits. A name carrying a path separator or a parent reference is refused outright
   * rather than scrubbed, so a client never silently writes somewhere it did not name.
   */
  public static String safeName(String raw) {
    if (raw == null) {
      return null;
    }
    String name = raw.trim();
    if (name.isEmpty() || name.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
      return null;
    }
    if (name.contains("/") || name.contains("\\") || name.contains("..")) {
      return null;
    }
    // A control character in a name would forge lines in the log that records it.
    for (int i = 0; i < name.length(); i++) {
      if (Character.isISOControl(name.charAt(i))) {
        return null;
      }
    }
    return name;
  }

  /**
   * The folder holding one project's uploaded files: {@code <workspace>/<projectId>}
   * under the target plugin's connection. Scheme-agnostic; the connection supplies it.
   */
  public static Path projectDir(UploadConfig cfg, FileSystemConfig fsConfig, String projectId) {
    Map<String, WorkspaceConfig> workspaces = fsConfig.getWorkspaces();
    WorkspaceConfig ws = workspaces == null ? null : workspaces.get(cfg.workspace);
    if (ws == null || ws.getLocation() == null) {
      return null;
    }
    return new Path(new Path(fsConfig.getConnection()),
        new Path(ws.getLocation(), projectId));
  }

  // ==================== Endpoint ====================

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Upload a file into a project",
      description = "Writes the part into the project's folder in the cluster upload target")
  public Response upload(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "Replace an existing file of the same name")
          @QueryParam("overwrite") boolean overwrite,
      @FormDataParam("file") InputStream part,
      @FormDataParam("file") FormDataContentDisposition partMeta) {

    PersistentStore<Project> store = ProjectResources.openStore(storeProvider, workManager);
    Project project = store.get(id);
    if (project == null || project.getDeletedAt() > 0) {
      return error(Response.Status.NOT_FOUND, "Project not found");
    }
    String user = principal.getName();
    if (!ProjectResources.canWrite(project, user)) {
      return error(Response.Status.FORBIDDEN, "You do not have write access to this project");
    }

    if (part == null || partMeta == null) {
      return error(Response.Status.BAD_REQUEST, "No file part named 'file' was supplied");
    }
    String name = safeName(partMeta.getFileName());
    if (name == null) {
      return error(Response.Status.BAD_REQUEST, "Invalid file name: it is empty, longer "
          + "than " + MAX_NAME_BYTES + " bytes, or contains '/', '\\', '..' or a control "
          + "character");
    }

    UploadConfig cfg = UploadConfigResources.current(storeProvider);
    if (cfg == null || cfg.plugin == null || cfg.workspace == null) {
      return error(Response.Status.CONFLICT, NOT_CONFIGURED);
    }
    FileSystemPlugin fsPlugin = fileSystemPlugin(cfg.plugin);
    if (fsPlugin == null) {
      return error(Response.Status.CONFLICT, NOT_CONFIGURED
          + " The configured plugin '" + cfg.plugin + "' is no longer a file system plugin.");
    }
    FileSystemConfig fsConfig = (FileSystemConfig) fsPlugin.getConfig();
    Path dir = projectDir(cfg, fsConfig, id);
    if (dir == null) {
      return error(Response.Status.CONFLICT, NOT_CONFIGURED
          + " The configured workspace '" + cfg.workspace + "' no longer exists.");
    }

    // Second, independent traversal guard: safeName lets through names that still do not
    // resolve inside the project folder, such as "." or a scheme-bearing "foo:bar" that
    // replaces the parent outright.
    Path target;
    try {
      target = new Path(dir, name);
    } catch (IllegalArgumentException e) {
      return error(Response.Status.BAD_REQUEST, "Invalid file name: it cannot be resolved");
    }
    String dirUri = dir.toUri().normalize().toString();
    if (!target.toUri().normalize().toString().startsWith(dirUri + "/")) {
      return error(Response.Status.BAD_REQUEST,
          "Invalid file name: it does not resolve inside the project folder");
    }

    String schema = cfg.plugin + "." + cfg.workspace + "." + id;
    Response written = writeAsUser(user, fsPlugin, fsConfig, dir, target, part,
        cfg.maxSizeBytes, overwrite, name, schema);
    if (written.getStatus() != Response.Status.CREATED.getStatusCode()) {
      return written;
    }

    registerFolderDataset(store, id, cfg);
    logger.info("User '{}' uploaded '{}' into project {} at {}", user, name, id, target);
    return written;
  }

  // ==================== Writing ====================

  /**
   * Performs the write as the uploading user when impersonation is on. A file written as
   * the Drillbit's own user on a secured cluster is unreadable by the person who uploaded
   * it, and that cannot be repaired through Drill afterwards.
   */
  private Response writeAsUser(String user, FileSystemPlugin fsPlugin, FileSystemConfig fsConfig,
      Path dir, Path target, InputStream part, long maxSizeBytes, boolean overwrite,
      String name, String schema) {
    PrivilegedExceptionAction<Response> write =
        () -> write(fsPlugin, fsConfig, dir, target, part, maxSizeBytes, overwrite, name, schema);
    try {
      if (workManager.getContext().getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED)) {
        return ImpersonationUtil.createProxyUgi(user).doAs(write);
      }
      return write.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DrillRuntimeException("Upload interrupted", e);
    } catch (Exception e) {
      // The detail stays in the log: file system messages carry server-side paths.
      logger.error("Failed to upload '{}' to {}", name, target, e);
      return error(Response.Status.INTERNAL_SERVER_ERROR, "Failed to write the uploaded file");
    }
  }

  private Response write(FileSystemPlugin fsPlugin, FileSystemConfig fsConfig,
      Path dir, Path target, InputStream part, long maxSizeBytes, boolean overwrite,
      String name, String schema) throws IOException {
    // Resolved inside the doAs so the file system is the uploading user's, not the
    // process user's: FileSystem instances are cached per UGI.
    FileSystem fs = FileSystem.get(URI.create(fsConfig.getConnection()), fsPlugin.getFsConf());

    if (!overwrite && fs.exists(target)) {
      return error(Response.Status.CONFLICT, "A file named '" + name
          + "' already exists in this project. Retry with overwrite=true to replace it.");
    }
    fs.mkdirs(dir);

    // The bytes land beside the target under a temporary name and are moved into place
    // only once they are all there, so an abort never costs the file already uploaded.
    Path staged = new Path(dir, name + STAGING_SUFFIX + UUID.randomUUID());

    // One byte past the cap is enough to know the part is too big, and the part is
    // never held in memory: these are data files.
    long limit = maxSizeBytes == Long.MAX_VALUE ? maxSizeBytes : maxSizeBytes + 1;
    long copied;
    try (OutputStream out = fs.create(staged, true)) {
      copied = IOUtils.copyLarge(part, out, 0L, limit);
    } catch (IOException e) {
      discard(fs, staged);
      throw e;
    }
    if (copied > maxSizeBytes) {
      discard(fs, staged);
      return error(Response.Status.REQUEST_ENTITY_TOO_LARGE,
          "File exceeds the maximum upload size of " + maxSizeBytes + " bytes");
    }

    commit(fs, staged, target, overwrite, name);

    return Response.status(Response.Status.CREATED)
        .entity(new UploadResponse(name, name, schema))
        .build();
  }

  /**
   * Moves the staged bytes onto the target. The way is cleared first, for an overwrite
   * only; every other case has already passed the collision check. Renaming optimistically
   * and clearing only on refusal would be worse: Hadoop's {@code rename} moves the source
   * <em>into</em> a directory that happens to sit at the target name instead of failing,
   * which would report a successful upload sitting at the wrong path.
   *
   * <p>A failure here never discards the staged file: it holds the only copy of what the
   * user uploaded, and leaving it under a recoverable name beats losing it. Its path goes
   * to the log so an administrator can put it right.
   */
  private static void commit(FileSystem fs, Path staged, Path target, boolean overwrite,
      String name) throws IOException {
    try {
      if (overwrite) {
        fs.delete(target, false);
      }
      if (fs.rename(staged, target)) {
        return;
      }
    } catch (IOException e) {
      logger.error("Could not move upload '{}' onto {}. The uploaded bytes remain at {}",
          name, target, staged, e);
      throw new IOException("Could not move the uploaded file into place");
    }
    logger.error("Could not move upload '{}' onto {}. The uploaded bytes remain at {}",
        name, target, staged);
    throw new IOException("Could not move the uploaded file into place");
  }

  private static void discard(FileSystem fs, Path staged) {
    try {
      fs.delete(staged, false);
    } catch (IOException e) {
      logger.warn("Failed to remove partial upload {}: {}", staged, e.getMessage());
    }
  }

  // ==================== Project bookkeeping ====================

  /**
   * Gives the project its one {@code folder} dataset reference, so the uploaded files
   * show up as a queryable table. Serialised on the interned id like the other project
   * mutations, and a no-op once the project already has a folder reference.
   */
  private void registerFolderDataset(PersistentStore<Project> store, String id, UploadConfig cfg) {
    synchronized (id.intern()) {
      Project project = store.get(id);
      if (project == null) {
        return;
      }
      boolean hasFolder = project.getDatasets().stream()
          .anyMatch(d -> FOLDER_DATASET_TYPE.equals(d.getType()));
      if (hasFolder) {
        return;
      }
      project.getDatasets().add(new DatasetRef(UUID.randomUUID().toString(),
          FOLDER_DATASET_TYPE, cfg.plugin + "." + cfg.workspace, id, null,
          FOLDER_DATASET_LABEL));
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);
    }
  }

  // ==================== Small helpers ====================

  private FileSystemPlugin fileSystemPlugin(String pluginName) {
    StoragePlugin plugin;
    try {
      plugin = storageRegistry.getPlugin(pluginName);
    } catch (Exception e) {
      logger.warn("Could not load upload target plugin '{}': {}", pluginName, e.getMessage());
      return null;
    }
    return plugin instanceof FileSystemPlugin ? (FileSystemPlugin) plugin : null;
  }

  private static Response error(Response.Status status, String message) {
    return Response.status(status).entity(new MessageResponse(message)).build();
  }
}
