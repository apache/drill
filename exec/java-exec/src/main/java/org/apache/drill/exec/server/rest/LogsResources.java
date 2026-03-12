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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.work.WorkManager;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class LogsResources {
  private static final Logger logger = LoggerFactory.getLogger(LogsResources.class);

  private static final String DFS_PLUGIN_NAME = "dfs";
  private static final String LOGS_WORKSPACE_NAME = "logs";
  private static final String DRILL_LOG_FORMAT_NAME = "drilllog";

  // Regex to parse Drill's default logback format:
  // %date{ISO8601} [%thread] %-5level %logger{36} - %msg%n
  // Example: 2025-03-12T10:30:45,123 [main] INFO  o.a.d.e.s.DrillbitContext - Starting
  private static final String DRILL_LOG_REGEX =
      "(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2},\\d+)\\s+\\[([^\\]]+)\\]\\s+(\\w+)\\s+([^\\s]+)\\s+-\\s+(.*)";

  @Inject WorkManager work;

  private static final FileFilter file_filter = new FileFilter() {
    @Override
    public boolean accept(File file) {
      return file.isFile();
    }
  };
  private static final DateTimeFormatter format = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");


  @GET
  @Path("/logs")
  @Produces(MediaType.TEXT_HTML)
  public Response getLogs() {
    return Response.seeOther(java.net.URI.create("/sqllab#/logs")).build();
  }

  @GET
  @Path("/logs.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLogsJSON() {
    Set<Log> logs = Sets.newTreeSet();

    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      return Response.ok(logs).build();
    }

    File folder = new File(logDir);
    if (!folder.isDirectory()) {
      logger.warn("DRILL_LOG_DIR does not point to a valid directory: {}", logDir);
      return Response.ok(logs).build();
    }

    File[] files = folder.listFiles(file_filter);
    if (files != null) {
      for (File file : files) {
        logs.add(new Log(file.getName(), file.length(), file.lastModified()));
      }
    }

    return Response.ok(logs).build();
  }

  @GET
  @Path("/log/{name}/content")
  @Produces(MediaType.TEXT_HTML)
  public Response getLog(@PathParam("name") String name) {
    return Response.seeOther(java.net.URI.create("/sqllab#/logs")).build();
  }

  @GET
  @Path("/log/{name}/content.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLogJSON(@PathParam("name") final String name) throws IOException {
    File folder = getLogFolderSafe();
    if (folder == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(new LogSetupResponse(false, "DRILL_LOG_DIR is not configured"))
          .build();
    }
    File file = getFileByName(folder, name);

    final int maxLines = work.getContext().getOptionManager().getOption(ExecConstants.WEB_LOGS_MAX_LINES).num_val.intValue();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      @SuppressWarnings("serial")
      Map<Integer, String> cache = new LinkedHashMap<Integer, String>(maxLines, .75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
          return size() > maxLines;
        }
      };

      String line;
      int i = 0;
      while ((line = br.readLine()) != null) {
        cache.put(i++, line);
      }

      return Response.ok(new LogContent(file.getName(), cache.values(), maxLines)).build();
    }
  }

  @GET
  @Path("/log/{name}/download")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getFullLog(@PathParam("name") final String name) {
    File folder = getLogFolderSafe();
    if (folder == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
    File file = getFileByName(folder, name);
    return Response.ok(file)
        .header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment;filename=\"%s\"", name))
        .build();
  }

  @GET
  @Path("/api/v1/logs/sql-status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLogsSqlStatus() {
    Map<String, Object> status = new LinkedHashMap<>();

    // Check if DRILL_LOG_DIR is set
    String logDir = System.getenv("DRILL_LOG_DIR");
    status.put("logDirConfigured", logDir != null);
    status.put("logDir", logDir);

    // Check if the dfs.logs workspace exists
    boolean workspaceExists = false;
    boolean formatExists = false;
    try {
      StoragePluginRegistry storage = work.getContext().getStorage();
      StoragePluginConfig config = storage.getStoredConfig(DFS_PLUGIN_NAME);
      if (config instanceof FileSystemConfig) {
        FileSystemConfig fsConfig = (FileSystemConfig) config;
        workspaceExists = fsConfig.getWorkspaces().containsKey(LOGS_WORKSPACE_NAME);
        formatExists = fsConfig.getFormats().containsKey(DRILL_LOG_FORMAT_NAME);
      }
    } catch (Exception e) {
      logger.debug("Error checking logs SQL status", e);
    }

    status.put("workspaceExists", workspaceExists);
    status.put("formatExists", formatExists);
    status.put("ready", workspaceExists && formatExists);

    return Response.ok(status).build();
  }

  @POST
  @Path("/api/v1/logs/sql-setup")
  @Produces(MediaType.APPLICATION_JSON)
  public Response setupLogsSql() {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new LogSetupResponse(false,
              "DRILL_LOG_DIR environment variable is not set"))
          .build();
    }

    try {
      StoragePluginRegistry storage = work.getContext().getStorage();
      StoragePluginConfig config = storage.getStoredConfig(DFS_PLUGIN_NAME);

      if (!(config instanceof FileSystemConfig)) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(new LogSetupResponse(false,
                "dfs plugin is not a file system plugin"))
            .build();
      }

      FileSystemConfig fsConfig = (FileSystemConfig) config;
      boolean modified = false;

      // Add the logs workspace if it doesn't exist
      if (!fsConfig.getWorkspaces().containsKey(LOGS_WORKSPACE_NAME)) {
        FileSystemConfig copy = fsConfig.copy();
        copy.getWorkspaces().put(LOGS_WORKSPACE_NAME,
            new WorkspaceConfig(logDir, false, DRILL_LOG_FORMAT_NAME, false));
        storage.put(DFS_PLUGIN_NAME, copy);
        modified = true;
        logger.info("Created dfs.logs workspace pointing to {}", logDir);
      }

      // Add the drilllog format if it doesn't exist
      StoragePluginConfig updatedConfig = storage.getStoredConfig(DFS_PLUGIN_NAME);
      if (updatedConfig instanceof FileSystemConfig) {
        FileSystemConfig updatedFsConfig = (FileSystemConfig) updatedConfig;
        if (!updatedFsConfig.getFormats().containsKey(DRILL_LOG_FORMAT_NAME)) {
          // Build the format config as JSON to avoid a compile-time dependency
          // on the contrib/format-log module
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
          modified = true;
          logger.info("Created drilllog format plugin for parsing Drill logs");
        }
      }

      String msg = modified
          ? "Log SQL workspace and format configured successfully"
          : "Log SQL workspace and format were already configured";
      return Response.ok(new LogSetupResponse(true, msg)).build();

    } catch (Exception e) {
      logger.error("Error setting up logs SQL workspace", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new LogSetupResponse(false,
              "Failed to configure: " + e.getMessage()))
          .build();
    }
  }

  public static class LogSetupResponse {
    @JsonProperty
    public final boolean success;
    @JsonProperty
    public final String message;

    public LogSetupResponse(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }

  private File getLogFolder() {
    return new File(Preconditions.checkNotNull(System.getenv("DRILL_LOG_DIR"), "DRILL_LOG_DIR variable is not set"));
  }

  /**
   * Returns the log folder if DRILL_LOG_DIR is set and valid, or null otherwise.
   */
  private File getLogFolderSafe() {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      return null;
    }
    File folder = new File(logDir);
    if (!folder.isDirectory()) {
      return null;
    }
    return folder;
  }

  private File getFileByName(File folder, final String name) {
    // Prevent path traversal attacks
    if (name.contains("..") || name.contains("/") || name.contains("\\")) {
      throw new DrillRuntimeException("Invalid log file name: " + name);
    }

    File[] files = folder.listFiles((dir, fileName) -> fileName.equals(name));
    if (files == null || files.length == 0) {
      throw new DrillRuntimeException(name + " doesn't exist");
    }
    return files[0];
  }

  @XmlRootElement
  public class Log implements Comparable<Log> {

    private final String name;
    private final long size;
    private final DateTime lastModified;

    @JsonCreator
    public Log (@JsonProperty("name") String name,
                @JsonProperty("size") long size,
                @JsonProperty("lastModified") long lastModified) {
      this.name = name;
      this.size = size;
      this.lastModified = new DateTime(lastModified);
    }

    public String getName() {
      return name;
    }

    public String getSize() {
      return Math.ceil(size / 1024d) + " KB";
    }

    public String getLastModified() {
      return lastModified.toString(format);
    }

    @Override
    public int compareTo(Log log) {
      return this.getName().compareTo(log.getName());
    }
  }

  @XmlRootElement
  public class LogContent {
    private final String name;
    private final Collection<String> lines;
    private final int maxLines;

    @JsonCreator
    public LogContent (@JsonProperty("name") String name,
                       @JsonProperty("lines") Collection<String> lines,
                       @JsonProperty("maxLines") int maxLines) {
      this.name = name;
      this.lines = lines;
      this.maxLines = maxLines;
    }

    public String getName() {
      return name;
    }

    public Collection<String> getLines() { return lines; }

    public int getMaxLines() { return maxLines; }
  }
}
