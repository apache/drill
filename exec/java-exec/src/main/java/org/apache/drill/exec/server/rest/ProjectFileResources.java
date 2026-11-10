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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NameBinding;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
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
import org.apache.hadoop.fs.FileStatus;
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
import java.net.URLEncoder;
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

  static final String FOLDER_DATASET_TYPE = "folder";
  /** Headroom for the multipart boundaries and part headers around the file itself. */
  private static final long MULTIPART_ENVELOPE_BYTES = 8192;

  /** {@code .upload-} plus a UUID, the suffix a staged write adds to the name. */
  static final String STAGING_SUFFIX = ".upload-";
  private static final int STAGING_SUFFIX_BYTES = STAGING_SUFFIX.length() + 36;
  /**
   * The longest name accepted, leaving room for the staging suffix inside the 255 bytes
   * that file systems commonly allow a single path component. Without this a name near
   * the limit would pass here and then fail at create time as a 500.
   */
  private static final int MAX_NAME_BYTES = 255 - STAGING_SUFFIX_BYTES;
  private static final String FOLDER_DATASET_LABEL = "Uploaded files";

  /** Shown whenever the cluster has no usable upload target. */
  /** Machine-readable reasons carried in the {@code code} field of a 409 body. */
  static final String CODE_EXISTS = "EXISTS";
  static final String CODE_NOT_CONFIGURED = "NOT_CONFIGURED";

  /**
   * A project id that cannot be a path component. Scratch project ids carry the owner's
   * principal name, so this is reachable without a crafted request.
   */
  private static final String INVALID_PROJECT_ID =
      "Invalid project id: it cannot be used as a folder name";

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

  @Inject
  WebUserConnection webUserConnection;

  /** The 201 body: where the uploaded file can be queried. */
  public static class UploadResponse {
    @JsonProperty
    public final String name;

    @JsonProperty
    public final String path;

    @JsonProperty
    public final String schema;

    /**
     * The one field a client can put in a query: the file-level table path, quoted the
     * way {@link ProjectSchemaCache#quoteTablePath} quotes it, e.g.
     * {@code dfs.`uploads`.`3fa85f64-5717-.../sales.csv`}. {@link #schema} names the
     * project's folder and leaves the id unquoted, so it is not valid Drill SQL on its
     * own: a project id is a UUID, whose hyphens parse as subtraction and whose first
     * segment may begin with a digit.
     */
    @JsonProperty
    public final String tablePath;

    public UploadResponse(String name, String path, String schema, String tablePath) {
      this.name = name;
      this.path = path;
      this.schema = schema;
      this.tablePath = tablePath;
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

  /** The configured upload target's workspace, or null when it no longer exists. */
  static WorkspaceConfig uploadWorkspace(UploadConfig cfg, FileSystemConfig fsConfig) {
    Map<String, WorkspaceConfig> workspaces = fsConfig.getWorkspaces();
    return workspaces == null ? null : workspaces.get(cfg.workspace);
  }

  /**
   * The folder holding one project's uploaded files: {@code <workspace>/<projectId>}
   * under the target plugin's connection. Scheme-agnostic; the connection supplies it.
   *
   * <p>The project id passes the same two guards every file name passes. Most ids are
   * UUIDs, but a scratch project's id is a fixed prefix plus the owner's principal name
   * ({@code ProjectResources.getOrCreateScratch}), and a principal carrying a {@code /}
   * would nest the folder while one carrying {@code ..} would leave the workspace
   * altogether. An id that fails either guard yields null, the same as a workspace that
   * has gone away, and the callers refuse the request.
   */
  public static Path projectDir(UploadConfig cfg, FileSystemConfig fsConfig, String projectId) {
    WorkspaceConfig ws = uploadWorkspace(cfg, fsConfig);
    if (ws == null || ws.getLocation() == null || safeName(projectId) == null) {
      return null;
    }
    Path base = new Path(new Path(fsConfig.getConnection()), ws.getLocation());
    try {
      return resolveInside(base, projectId);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * The path of {@code name} inside {@code dir}, or an {@link IllegalArgumentException}
   * whose message is safe to return to the client. The second, independent traversal
   * guard behind {@link #safeName}: it lets through names that still do not resolve
   * inside the project folder, such as "." or a scheme-bearing "foo:bar" that replaces
   * the parent outright. Upload, delete and download all resolve through here.
   */
  static Path resolveInside(Path dir, String name) {
    Path target;
    try {
      target = new Path(dir, name);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid file name: it cannot be resolved");
    }
    String dirUri = dir.toUri().normalize().toString();
    if (!target.toUri().normalize().toString().startsWith(dirUri + "/")) {
      throw new IllegalArgumentException(
          "Invalid file name: it does not resolve inside the project folder");
    }
    return target;
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

    if (safeName(id) == null) {
      return error(Response.Status.BAD_REQUEST, INVALID_PROJECT_ID);
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
      return error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED);
    }
    FileSystemPlugin fsPlugin = fileSystemPlugin(cfg.plugin);
    if (fsPlugin == null) {
      return error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED
          + " The configured plugin '" + cfg.plugin + "' is no longer a file system plugin.");
    }
    FileSystemConfig fsConfig = (FileSystemConfig) fsPlugin.getConfig();
    Path dir = projectDir(cfg, fsConfig, id);
    if (dir == null) {
      return error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED
          + " The configured workspace '" + cfg.workspace + "' no longer exists.");
    }
    // Re-checked here, not only when the setting was saved. "writable" is a Drill
    // planner concept and not a file system permission, so nothing in fs.create would
    // refuse a write to a workspace an administrator has since marked read-only.
    WorkspaceConfig ws = uploadWorkspace(cfg, fsConfig);
    if (ws == null || !ws.isWritable()) {
      return error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED
          + " The configured workspace '" + cfg.workspace + "' is not writable.");
    }

    Path target;
    try {
      target = resolveInside(dir, name);
    } catch (IllegalArgumentException e) {
      return error(Response.Status.BAD_REQUEST, e.getMessage());
    }

    UploadResponse created = new UploadResponse(name, name,
        cfg.plugin + "." + cfg.workspace + "." + id,
        ProjectSchemaCache.quoteTablePath(uploadSchema(cfg), id + "/" + name));
    Response written = writeAsUser(user, fsPlugin, fsConfig, dir, target, part,
        cfg.maxSizeBytes, overwrite, name, created);
    if (written.getStatus() != Response.Status.CREATED.getStatusCode()) {
      return written;
    }

    registerFolderDataset(store, id, cfg);
    cacheUploadedFile(id, cfg, name);
    logger.info("User '{}' uploaded '{}' into project {} at {}", user, name, id, target);
    return written;
  }

  // ==================== Delete and download ====================

  /** A request resolved to one file inside a project's folder, or the reason it was refused. */
  private static final class Located {
    Response refusal;
    FileSystemPlugin fsPlugin;
    FileSystemConfig fsConfig;
    Path target;
    String name;
    String user;
    UploadConfig cfg;
  }

  /**
   * Runs the checks delete and download share: the project exists, the caller has the
   * access asked for, the name passes {@link #safeName} and then the second traversal
   * guard, and an upload target is configured. Error bodies are fixed text so a missing
   * file never reveals whether the project folder exists.
   */
  private Located locate(String id, String rawName, boolean needWrite) {
    Located loc = new Located();
    PersistentStore<Project> store = ProjectResources.openStore(storeProvider, workManager);
    Project project = store.get(id);
    if (project == null || project.getDeletedAt() > 0) {
      loc.refusal = error(Response.Status.NOT_FOUND, "Project not found");
      return loc;
    }
    loc.user = principal.getName();
    if (needWrite) {
      if (!ProjectResources.canWrite(project, loc.user)) {
        loc.refusal = error(Response.Status.FORBIDDEN,
            "You do not have write access to this project");
        return loc;
      }
    } else if (!ProjectResources.canRead(project, loc.user)) {
      loc.refusal = error(Response.Status.FORBIDDEN,
          "You do not have access to this project");
      return loc;
    }

    if (safeName(id) == null) {
      loc.refusal = error(Response.Status.BAD_REQUEST, INVALID_PROJECT_ID);
      return loc;
    }

    loc.name = safeName(rawName);
    if (loc.name == null) {
      loc.refusal = error(Response.Status.BAD_REQUEST, "Invalid file name: it is empty, "
          + "longer than " + MAX_NAME_BYTES + " bytes, or contains '/', '\\', '..' or a "
          + "control character");
      return loc;
    }

    UploadConfig cfg = UploadConfigResources.current(storeProvider);
    if (cfg == null || cfg.plugin == null || cfg.workspace == null) {
      loc.refusal = error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED);
      return loc;
    }
    loc.cfg = cfg;
    loc.fsPlugin = fileSystemPlugin(cfg.plugin);
    if (loc.fsPlugin == null) {
      loc.refusal = error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED);
      return loc;
    }
    loc.fsConfig = (FileSystemConfig) loc.fsPlugin.getConfig();
    Path dir = projectDir(cfg, loc.fsConfig, id);
    if (dir == null) {
      loc.refusal = error(Response.Status.CONFLICT, CODE_NOT_CONFIGURED, NOT_CONFIGURED);
      return loc;
    }

    try {
      loc.target = resolveInside(dir, loc.name);
    } catch (IllegalArgumentException e) {
      loc.refusal = error(Response.Status.BAD_REQUEST, e.getMessage());
    }
    return loc;
  }

  /** Runs {@code action} as the requesting user when impersonation is on. */
  private <T> T asUser(String user, PrivilegedExceptionAction<T> action) throws Exception {
    if (workManager.getContext().getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED)) {
      return ImpersonationUtil.createProxyUgi(user).doAs(action);
    }
    return action.run();
  }

  @DELETE
  @jakarta.ws.rs.Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete an uploaded file from a project",
      description = "Removes one file from the project's folder in the cluster upload target")
  public Response delete(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "File name") @PathParam("name") String rawName) {
    Located loc = locate(id, rawName, true);
    if (loc.refusal != null) {
      return loc.refusal;
    }
    try {
      boolean deleted = asUser(loc.user, () -> {
        FileSystem fs = FileSystem.get(URI.create(loc.fsConfig.getConnection()),
            loc.fsPlugin.getFsConf());
        return isFile(fs, loc.target) && fs.delete(loc.target, false);
      });
      if (!deleted) {
        return error(Response.Status.NOT_FOUND, "File not found");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DrillRuntimeException("Delete interrupted", e);
    } catch (Exception e) {
      logger.error("Failed to delete '{}' at {}", loc.name, loc.target, e);
      return error(Response.Status.INTERNAL_SERVER_ERROR, "Failed to delete the file");
    }
    uncacheDeletedFile(id, loc.cfg, loc.name);
    logger.info("User '{}' deleted '{}' from project {} at {}", loc.user, loc.name, id,
        loc.target);
    return Response.noContent().build();
  }

  @GET
  @jakarta.ws.rs.Path("/{name}/content")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  @Operation(summary = "Download an uploaded file from a project",
      description = "Streams the file's bytes through the Drillbit from the upload target")
  public Response download(
      @Parameter(description = "Project ID") @PathParam("id") String id,
      @Parameter(description = "File name") @PathParam("name") String rawName) {
    Located loc = locate(id, rawName, false);
    if (loc.refusal != null) {
      return loc.refusal;
    }
    try {
      // Checked here so a missing file is still a 404. The stream is not opened until the
      // body is written, so its lifetime is the write's: a HEAD, or a failure before the
      // entity runs, never holds a handle.
      boolean present = asUser(loc.user, () -> isFile(openFs(loc), loc.target));
      if (!present) {
        return error(Response.Status.NOT_FOUND, "File not found");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DrillRuntimeException("Download interrupted", e);
    } catch (Exception e) {
      logger.error("Failed to open '{}' at {}", loc.name, loc.target, e);
      return error(Response.Status.INTERNAL_SERVER_ERROR, "Failed to read the file");
    }
    StreamingOutput body = out -> {
      try {
        try (InputStream source = asUser(loc.user, () -> openFs(loc).open(loc.target))) {
          IOUtils.copyLarge(source, out);
        }
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        logger.error("Failed to stream '{}' at {}", loc.name, loc.target, e);
        throw new IOException("Failed to read the file");
      }
    };
    return Response.ok(body, MediaType.APPLICATION_OCTET_STREAM)
        .header("Content-Disposition", contentDisposition(loc.name))
        .build();
  }

  private static FileSystem openFs(Located loc) throws IOException {
    return FileSystem.get(URI.create(loc.fsConfig.getConnection()), loc.fsPlugin.getFsConf());
  }

  private static boolean isFile(FileSystem fs, Path path) throws IOException {
    try {
      FileStatus status = fs.getFileStatus(path);
      return status.isFile();
    } catch (java.io.FileNotFoundException e) {
      return false;
    }
  }

  /** An attachment header with an ASCII fallback name and the exact one as RFC 5987. */
  private static String contentDisposition(String name) {
    String ascii = name.replaceAll("[^\\x20-\\x7e]|[\"\\\\]", "_");
    String encoded = URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20");
    return "attachment; filename=\"" + ascii + "\"; filename*=UTF-8''" + encoded;
  }

  // ==================== Writing ====================

  /**
   * Performs the write as the uploading user when impersonation is on. A file written as
   * the Drillbit's own user on a secured cluster is unreadable by the person who uploaded
   * it, and that cannot be repaired through Drill afterwards.
   */
  private Response writeAsUser(String user, FileSystemPlugin fsPlugin, FileSystemConfig fsConfig,
      Path dir, Path target, InputStream part, long maxSizeBytes, boolean overwrite,
      String name, UploadResponse created) {
    PrivilegedExceptionAction<Response> write =
        () -> write(fsPlugin, fsConfig, dir, target, part, maxSizeBytes, overwrite, name,
            created);
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
      String name, UploadResponse created) throws IOException {
    // Resolved inside the doAs so the file system is the uploading user's, not the
    // process user's: FileSystem instances are cached per UGI.
    FileSystem fs = FileSystem.get(URI.create(fsConfig.getConnection()), fsPlugin.getFsConf());

    if (!overwrite && fs.exists(target)) {
      return error(Response.Status.CONFLICT, CODE_EXISTS, "A file named '" + name
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
        .entity(created)
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
   * mutations, and a no-op once the project already has a folder reference
   * <em>on this folder</em>.
   *
   * <p>The match is on type, schema and table together, not type alone. An administrator
   * who repoints the cluster target from {@code dfs.uploads} to {@code s3.uploads} leaves
   * every project that has already uploaded holding a reference to the old schema; if
   * type alone were enough, those projects would never gain one for the new schema, so
   * their later uploads would land somewhere the project cannot see while the tree went
   * on showing the dead folder. A project may therefore hold one reference per folder it
   * has uploaded into, and {@code uploadFolders} in {@code ProjectResources} already
   * scans a schema's folder references together.
   */
  private void registerFolderDataset(PersistentStore<Project> store, String id, UploadConfig cfg) {
    String schema = uploadSchema(cfg);
    synchronized (id.intern()) {
      Project project = store.get(id);
      if (project == null) {
        return;
      }
      boolean hasFolder = project.getDatasets().stream()
          .anyMatch(d -> FOLDER_DATASET_TYPE.equals(d.getType())
              && schema.equals(d.getSchema()) && id.equals(d.getTable()));
      if (hasFolder) {
        return;
      }
      project.getDatasets().add(new DatasetRef(UUID.randomUUID().toString(),
          FOLDER_DATASET_TYPE, schema, id, null,
          FOLDER_DATASET_LABEL));
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);
    }
  }

  /**
   * Puts the file just written into the project's schema cache, so it is in autocomplete
   * and in the AI assistant's context without waiting out
   * {@link ProjectSchemaCache#STALE_INTERVAL_MS}.
   *
   * <p>Only this one file is probed. Re-scanning the whole folder here would run one
   * query per file it holds, on the upload's own request thread and inside the cache's
   * monitor, so a project with a few hundred files would pay seconds on every upload and
   * delete and would stall every other project's scanning while it did. The whole-folder
   * {@link ProjectSchemaCache#fileScan} stays the scanner for the dataset-scan path,
   * where a sweep is what is being asked for.
   *
   * <p>Best-effort: a cache failure must never fail a write that has already landed.
   */
  private void cacheUploadedFile(String id, UploadConfig cfg, String name) {
    String schema = uploadSchema(cfg);
    if (!ProjectSchemaCache.isScannable(storageRegistry, schema)) {
      return;
    }
    try {
      ProjectSchemaCache cache = ProjectSchemaCache.get(storeProvider, workManager);
      // Probed before the merge takes the cache's monitor, never while holding it.
      cache.putTable(id, schema,
          ProjectSchemaCache.fileEntry(schema, id + "/" + name, workManager,
              webUserConnection));
    } catch (Exception e) {
      logger.warn("Could not cache the uploaded file '{}' for project {} schema {}: {}",
          name, id, schema, e.getMessage());
    }
  }

  /** Drops a deleted file from the project's schema cache. No query is needed. */
  private void uncacheDeletedFile(String id, UploadConfig cfg, String name) {
    String schema = uploadSchema(cfg);
    if (!ProjectSchemaCache.isScannable(storageRegistry, schema)) {
      return;
    }
    try {
      ProjectSchemaCache.get(storeProvider, workManager)
          .removeTable(id, schema, id + "/" + name);
    } catch (Exception e) {
      logger.warn("Could not drop the deleted file '{}' from the schema cache for "
          + "project {} schema {}: {}", name, id, schema, e.getMessage());
    }
  }

  private static String uploadSchema(UploadConfig cfg) {
    return cfg.plugin + "." + cfg.workspace;
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

  /** A message body that also carries a stable, machine-readable code. */
  public static class CodedMessageResponse extends MessageResponse {
    @JsonProperty
    public String code;

    public CodedMessageResponse(String code, String message) {
      super(message);
      this.code = code;
    }
  }

  private static Response error(Response.Status status, String code, String message) {
    return Response.status(status).type(MediaType.APPLICATION_JSON)
        .entity(new CodedMessageResponse(code, message)).build();
  }

  private static Response error(Response.Status status, String message) {
    return Response.status(status).type(MediaType.APPLICATION_JSON)
        .entity(new MessageResponse(message)).build();
  }
}
