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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.BoxFileSystem;
import org.apache.drill.exec.store.dfs.DropboxFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.sftp.SFTPFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

/**
 * REST resource for testing storage plugin connections.
 * Supports FileSystem and JDBC plugin types.
 */
@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class TestConnectionResources {
  private static final Logger logger = LoggerFactory.getLogger(TestConnectionResources.class);

  private static final int CONNECTION_TIMEOUT_SECONDS = 10;

  @Inject
  StoragePluginRegistry storage;

  @POST
  @Path("/storage/test-connection")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public TestConnectionResult testConnection(PluginConfigWrapper pluginWrapper) {
    try {
      StoragePluginConfig cfg = pluginWrapper.getConfig();
      if (cfg instanceof FileSystemConfig) {
        return testFileSystemConnection((FileSystemConfig) cfg);
      } else if (isJdbcConfig(cfg)) {
        return testJdbcConnection(cfg);
      } else {
        return new TestConnectionResult(false,
          "Test connection is not supported for this plugin type: "
            + cfg.getClass().getSimpleName(), null);
      }
    } catch (Exception e) {
      logger.error("Unexpected error testing connection", e);
      return new TestConnectionResult(false,
        "Unexpected error: " + e.getMessage(),
        e.getClass().getName());
    }
  }

  private TestConnectionResult testFileSystemConnection(FileSystemConfig config) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Configuration conf = new Configuration();
      Optional.ofNullable(config.getConfig())
        .ifPresent(c -> c.forEach(conf::set));

      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, config.getConnection());
      conf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
      conf.set("fs.dropbox.impl", DropboxFileSystem.class.getName());
      conf.set("fs.sftp.impl", SFTPFileSystem.class.getName());
      conf.set("fs.box.impl", BoxFileSystem.class.getName());
      conf.set("fs.drill-local.impl", LocalSyncableFileSystem.class.getName());

      CredentialsProvider credentialsProvider = config.getCredentialsProvider();
      if (credentialsProvider != null) {
        credentialsProvider.getCredentials().forEach(conf::set);
      }

      Future<TestConnectionResult> future = executor.submit(new FsTestCallable(conf));
      return future.get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      return new TestConnectionResult(false,
        "Connection timed out after " + CONNECTION_TIMEOUT_SECONDS
          + " seconds. The endpoint may be unreachable.", null);
    } catch (Exception e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      return new TestConnectionResult(false,
        cause.getMessage(),
        cause.getClass().getName());
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Check if a config is a JDBC storage config without a compile-time dependency
   * on the contrib/storage-jdbc module.
   */
  private boolean isJdbcConfig(StoragePluginConfig cfg) {
    return "JdbcStorageConfig".equals(cfg.getClass().getSimpleName());
  }

  /**
   * Test a JDBC connection using reflection to access driver/url from the config,
   * since JdbcStorageConfig lives in the contrib/storage-jdbc module.
   */
  private TestConnectionResult testJdbcConnection(StoragePluginConfig config) {
    String driver;
    String url;
    try {
      Method getDriver = config.getClass().getMethod("getDriver");
      Method getUrl = config.getClass().getMethod("getUrl");
      driver = (String) getDriver.invoke(config);
      url = (String) getUrl.invoke(config);
    } catch (Exception e) {
      return new TestConnectionResult(false,
        "Unable to read JDBC config properties: " + e.getMessage(),
        e.getClass().getName());
    }

    // Load the JDBC driver class
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      return new TestConnectionResult(false,
        "JDBC driver class not found: " + driver
          + ". Make sure the driver JAR is in Drill's classpath.",
        e.getClass().getName());
    }

    // Get credentials
    String username = null;
    String password = null;
    CredentialsProvider credentialsProvider = config.getCredentialsProvider();
    if (credentialsProvider != null) {
      Optional<UsernamePasswordCredentials> creds =
        new UsernamePasswordCredentials.Builder()
          .setCredentialsProvider(credentialsProvider)
          .build();
      if (creds.isPresent()) {
        username = creds.get().getUsername();
        password = creds.get().getPassword();
      }
    }

    // Attempt to connect
    DriverManager.setLoginTimeout(CONNECTION_TIMEOUT_SECONDS);
    Connection conn = null;
    try {
      if (username != null) {
        conn = DriverManager.getConnection(url, username, password);
      } else {
        conn = DriverManager.getConnection(url);
      }
      return new TestConnectionResult(true,
        "Successfully connected to " + url, null);
    } catch (SQLException e) {
      return new TestConnectionResult(false,
        e.getMessage(),
        e.getClass().getName());
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          logger.warn("Error closing test JDBC connection", e);
        }
      }
    }
  }

  /**
   * Callable that performs the FileSystem connection test
   * so it can be executed with a timeout.
   */
  private static class FsTestCallable implements Callable<TestConnectionResult> {
    private final Configuration conf;

    FsTestCallable(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public TestConnectionResult call() throws Exception {
      FileSystem fs = null;
      try {
        fs = FileSystem.get(conf);
        fs.exists(new org.apache.hadoop.fs.Path("/"));
        return new TestConnectionResult(true,
          "Successfully connected to " + conf.get(FileSystem.FS_DEFAULT_NAME_KEY),
          null);
      } finally {
        if (fs != null) {
          try {
            fs.close();
          } catch (Exception e) {
            logger.warn("Error closing test FileSystem", e);
          }
        }
      }
    }

    private static final Logger logger = LoggerFactory.getLogger(FsTestCallable.class);
  }

  /**
   * JSON response for test-connection endpoint.
   */
  public static class TestConnectionResult {
    private final boolean success;
    private final String message;
    private final String errorClass;

    @JsonCreator
    public TestConnectionResult(
        @JsonProperty("success") boolean success,
        @JsonProperty("message") String message,
        @JsonProperty("errorClass") String errorClass) {
      this.success = success;
      this.message = message;
      this.errorClass = errorClass;
    }

    public boolean isSuccess() {
      return success;
    }

    public String getMessage() {
      return message;
    }

    public String getErrorClass() {
      return errorClass;
    }
  }
}
