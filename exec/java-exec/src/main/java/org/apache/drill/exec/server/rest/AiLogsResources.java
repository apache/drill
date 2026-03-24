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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * REST endpoint for logging AI interactions.
 * Writes logs to ai-interactions.log in the DRILL_LOG_DIR.
 * No authentication required - all users can log their AI interactions.
 */
@Path("/api/v1/ai/logs")
public class AiLogsResources {
  private static final Logger logger = LoggerFactory.getLogger(AiLogsResources.class);
  private static final String AI_LOG_FILE = "ai-interactions.log";
  private static final SimpleDateFormat ISO8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSS");

  /**
   * Log a batch of AI interactions.
   * Each log entry is written in Drill's standard log format:
   * %date{ISO8601} [thread] %-5level logger - message
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response logAiInteractions(List<AiLogEntry> entries) {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      logger.warn("DRILL_LOG_DIR is not set, cannot log AI interactions");
      return Response.ok(new AiLogResponse(false, "DRILL_LOG_DIR not configured")).build();
    }

    File file = new File(logDir, AI_LOG_FILE);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
      for (AiLogEntry entry : entries) {
        String logLine = formatLogLine(entry);
        writer.write(logLine);
        writer.newLine();
      }
      writer.flush();
      return Response.ok(new AiLogResponse(true, "Logged " + entries.size() + " AI interactions")).build();
    } catch (IOException e) {
      logger.error("Failed to write AI logs", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new AiLogResponse(false, "Failed to write logs: " + e.getMessage()))
          .build();
    }
  }

  /**
   * Format a log entry in Drill's standard log format with full details.
   * Format: YYYY-MM-DDTHH:MM:SS,mmm [thread] LEVEL org.apache.drill.exec.server.rest.AiLogsResources - message
   */
  private String formatLogLine(AiLogEntry entry) {
    Date timestamp = new Date(entry.timestamp);
    String dateStr = ISO8601_FORMAT.format(timestamp);
    String threadName = Thread.currentThread().getName();
    String level = "INFO";
    String loggerName = "org.apache.drill.exec.server.rest.AiLogsResources";

    // Build message with all relevant details
    StringBuilder message = new StringBuilder();
    message.append("type=").append(entry.type);
    message.append(" duration=").append(entry.duration).append("ms");
    message.append(" tokens=").append(entry.totalTokens);
    message.append(" (input=").append(entry.promptTokens);
    message.append(" output=").append(entry.responseTokens).append(")");
    message.append(" success=").append(entry.success);
    if (entry.error != null) {
      message.append(" error=").append(entry.error);
    }

    // Include prompt and response if available
    if (entry.prompt != null && !entry.prompt.isEmpty()) {
      // Truncate very long prompts to keep logs readable
      String promptStr = entry.prompt.length() > 500
          ? entry.prompt.substring(0, 500) + "..."
          : entry.prompt;
      message.append(" prompt=\"").append(escapeLogValue(promptStr)).append("\"");
    }
    if (entry.response != null && !entry.response.isEmpty()) {
      // Truncate very long responses
      String responseStr = entry.response.length() > 500
          ? entry.response.substring(0, 500) + "..."
          : entry.response;
      message.append(" response=\"").append(escapeLogValue(responseStr)).append("\"");
    }

    return String.format("%s [%s] %-5s %s - %s",
        dateStr, threadName, level, loggerName, message.toString());
  }

  /**
   * Escape special characters in log values to prevent log injection
   */
  private String escapeLogValue(String value) {
    if (value == null) {
      return "";
    }
    // Replace newlines with spaces, escape quotes
    return value.replace("\n", " ").replace("\r", " ").replace("\"", "\\\"");
  }

  /**
   * Request body for logging AI interactions.
   */
  public static class AiLogEntry {
    @JsonProperty public long timestamp;
    @JsonProperty public String type;
    @JsonProperty public int promptTokens;
    @JsonProperty public int responseTokens;
    @JsonProperty public int totalTokens;
    @JsonProperty public long duration;
    @JsonProperty public boolean success;
    @JsonProperty public String error;
    @JsonProperty public String prompt;
    @JsonProperty public String response;

    // Getters for Jackson
    public long getTimestamp() { return timestamp; }
    public String getType() { return type; }
    public int getPromptTokens() { return promptTokens; }
    public int getResponseTokens() { return responseTokens; }
    public int getTotalTokens() { return totalTokens; }
    public long getDuration() { return duration; }
    public boolean isSuccess() { return success; }
    public String getError() { return error; }
    public String getPrompt() { return prompt; }
    public String getResponse() { return response; }
  }

  /**
   * Response object for log write operations.
   */
  public static class AiLogResponse {
    @JsonProperty public final boolean success;
    @JsonProperty public final String message;

    public AiLogResponse(boolean success, String message) {
      this.success = success;
      this.message = message;
    }

    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
  }
}
