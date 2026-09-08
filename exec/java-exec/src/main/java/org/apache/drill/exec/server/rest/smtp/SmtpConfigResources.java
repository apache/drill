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
package org.apache.drill.exec.server.rest.smtp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.SSLSocketFactory;

/**
 * REST resource for managing SMTP email configuration.
 * Admin-only access. Passwords are redacted in GET responses.
 */
@Path("/api/v1/smtp/config")
@Tag(name = "SMTP Configuration", description = "Admin configuration for email alerting")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class SmtpConfigResources {

  private static final Logger logger = LoggerFactory.getLogger(SmtpConfigResources.class);
  private static final String CONFIG_STORE_NAME = "drill.sqllab.smtp_config";
  private static final String CONFIG_KEY = "default";
  private static final int SMTP_TIMEOUT_MS = 10000;

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<SmtpConfig> cachedStore;

  // ==================== Response Models ====================

  public static class ConfigResponse {
    @JsonProperty
    public String host;

    @JsonProperty
    public int port;

    @JsonProperty
    public String username;

    @JsonProperty
    public boolean passwordSet;

    @JsonProperty
    public String fromAddress;

    @JsonProperty
    public String fromName;

    @JsonProperty
    public String encryption;

    @JsonProperty
    public boolean enabled;

    public ConfigResponse() {
    }

    public ConfigResponse(SmtpConfig config) {
      this.host = config.getHost();
      this.port = config.getPort();
      this.username = config.getUsername();
      this.passwordSet = config.getPassword() != null && !config.getPassword().isEmpty();
      this.fromAddress = config.getFromAddress();
      this.fromName = config.getFromName();
      this.encryption = config.getEncryption();
      this.enabled = config.isEnabled();
    }
  }

  public static class UpdateConfigRequest {
    @JsonProperty
    public String host;

    @JsonProperty
    public Integer port;

    @JsonProperty
    public String username;

    @JsonProperty
    public String password;

    @JsonProperty
    public String fromAddress;

    @JsonProperty
    public String fromName;

    @JsonProperty
    public String encryption;

    @JsonProperty
    public Boolean enabled;

    public UpdateConfigRequest() {
    }

    @JsonCreator
    public UpdateConfigRequest(
        @JsonProperty("host") String host,
        @JsonProperty("port") Integer port,
        @JsonProperty("username") String username,
        @JsonProperty("password") String password,
        @JsonProperty("fromAddress") String fromAddress,
        @JsonProperty("fromName") String fromName,
        @JsonProperty("encryption") String encryption,
        @JsonProperty("enabled") Boolean enabled) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.fromAddress = fromAddress;
      this.fromName = fromName;
      this.encryption = encryption;
      this.enabled = enabled;
    }
  }

  public static class TestRequest {
    @JsonProperty
    public String recipientEmail;

    public TestRequest() {
    }

    @JsonCreator
    public TestRequest(@JsonProperty("recipientEmail") String recipientEmail) {
      this.recipientEmail = recipientEmail;
    }
  }

  public static class MessageResponse {
    @JsonProperty
    public boolean success;

    @JsonProperty
    public String message;

    public MessageResponse(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }

  // ==================== Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get SMTP configuration",
      description = "Returns SMTP configuration with password redacted")
  public Response getConfig() {
    logger.debug("Getting SMTP configuration");
    try {
      PersistentStore<SmtpConfig> store = getStore();
      SmtpConfig config = store.get(CONFIG_KEY);

      if (config == null) {
        return Response.ok(new ConfigResponse(new SmtpConfig())).build();
      }

      return Response.ok(new ConfigResponse(config)).build();
    } catch (Exception e) {
      logger.error("Error reading SMTP config", e);
      throw new DrillRuntimeException("Failed to read SMTP configuration: " + e.getMessage(), e);
    }
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update SMTP configuration",
      description = "Updates SMTP configuration. Supports partial updates.")
  public Response updateConfig(UpdateConfigRequest request) {
    logger.debug("Updating SMTP configuration");

    try {
      PersistentStore<SmtpConfig> store = getStore();
      SmtpConfig existing = store.get(CONFIG_KEY);
      if (existing == null) {
        existing = new SmtpConfig();
      }

      // Apply partial updates
      if (request.host != null) {
        existing.setHost(request.host);
      }
      if (request.port != null) {
        existing.setPort(request.port);
      }
      if (request.username != null) {
        existing.setUsername(request.username);
      }
      // Only update password if provided and non-empty
      if (request.password != null && !request.password.isEmpty()) {
        existing.setPassword(request.password);
      }
      if (request.fromAddress != null) {
        existing.setFromAddress(request.fromAddress);
      }
      if (request.fromName != null) {
        existing.setFromName(request.fromName);
      }
      if (request.encryption != null) {
        existing.setEncryption(request.encryption);
      }
      if (request.enabled != null) {
        existing.setEnabled(request.enabled);
      }

      store.put(CONFIG_KEY, existing);

      return Response.ok(new ConfigResponse(existing)).build();
    } catch (Exception e) {
      logger.error("Error updating SMTP config", e);
      throw new DrillRuntimeException("Failed to update SMTP configuration: " + e.getMessage(), e);
    }
  }

  @POST
  @Path("/test")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Test SMTP connection",
      description = "Tests connectivity to the SMTP server using the saved configuration")
  public Response testConfig(TestRequest request) {
    logger.debug("Testing SMTP configuration");

    try {
      PersistentStore<SmtpConfig> store = getStore();
      SmtpConfig config = store.get(CONFIG_KEY);

      if (config == null) {
        return Response.ok(new MessageResponse(false,
            "SMTP is not configured. Please save settings first.")).build();
      }

      if (config.getHost() == null || config.getHost().isEmpty()) {
        return Response.ok(new MessageResponse(false,
            "SMTP host is not configured.")).build();
      }

      // Test SMTP connection with a socket-level handshake
      String result = testSmtpConnection(config);
      return Response.ok(new MessageResponse(true,
          "SMTP connection successful. Server responded: " + result)).build();
    } catch (Exception e) {
      logger.error("Error testing SMTP connection", e);
      return Response.ok(new MessageResponse(false,
          "SMTP connection failed: " + e.getMessage())).build();
    }
  }

  // ==================== Helper Methods ====================

  /**
   * Tests SMTP connectivity by opening a socket connection and performing
   * a basic SMTP EHLO handshake. For SSL connections, uses SSLSocketFactory.
   */
  private String testSmtpConnection(SmtpConfig config) throws Exception {
    String encryption = config.getEncryption() != null ? config.getEncryption().toLowerCase() : "starttls";
    boolean useSsl = "ssl".equals(encryption);

    Socket socket = null;
    try {
      if (useSsl) {
        socket = SSLSocketFactory.getDefault().createSocket();
      } else {
        socket = new Socket();
      }
      socket.setSoTimeout(SMTP_TIMEOUT_MS);
      socket.connect(new InetSocketAddress(config.getHost(), config.getPort()), SMTP_TIMEOUT_MS);

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      PrintWriter writer = new PrintWriter(
          new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

      // Read server greeting
      String greeting = reader.readLine();
      if (greeting == null || !greeting.startsWith("220")) {
        throw new RuntimeException("Unexpected server greeting: " + greeting);
      }

      // Send EHLO
      writer.println("EHLO drill.apache.org");
      writer.flush();

      // Read EHLO response (may be multi-line)
      StringBuilder ehloResponse = new StringBuilder();
      String line;
      do {
        line = reader.readLine();
        if (line == null) {
          break;
        }
        ehloResponse.append(line).append(" ");
      } while (line.length() > 3 && line.charAt(3) == '-');

      // Send QUIT
      writer.println("QUIT");
      writer.flush();

      return greeting;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (Exception ignored) {
          // no-op
        }
      }
    }
  }

  private PersistentStore<SmtpConfig> getStore() {
    if (cachedStore == null) {
      synchronized (SmtpConfigResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    SmtpConfig.class
                )
                .name(CONFIG_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access SMTP config store", e);
          }
        }
      }
    }
    return cachedStore;
  }
}
