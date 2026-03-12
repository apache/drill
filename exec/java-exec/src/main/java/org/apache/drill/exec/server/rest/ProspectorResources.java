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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.ai.ChatMessage;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.apache.drill.exec.server.rest.ai.LlmProvider;
import org.apache.drill.exec.server.rest.ai.LlmProviderRegistry;
import org.apache.drill.exec.server.rest.ai.ToolDefinition;
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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * REST resource for the Prospector chat endpoint.
 * Provides streaming SSE responses by proxying to the configured LLM provider.
 */
@Path("/api/v1/ai")
@Tag(name = "Prospector", description = "AI assistant for Apache Drill SQL Lab")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ProspectorResources {

  private static final Logger logger = LoggerFactory.getLogger(ProspectorResources.class);
  private static final String CONFIG_STORE_NAME = "drill.sqllab.ai_config";
  private static final String CONFIG_KEY = "default";

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<LlmConfig> cachedStore;

  // ==================== Request/Response Models ====================

  public static class AiStatusResponse {
    @JsonProperty
    public boolean enabled;

    @JsonProperty
    public boolean configured;

    public AiStatusResponse(boolean enabled, boolean configured) {
      this.enabled = enabled;
      this.configured = configured;
    }
  }

  public static class ChatRequest {
    @JsonProperty
    public List<ChatMessage> messages;

    @JsonProperty
    public List<ToolDefinition> tools;

    @JsonProperty
    public ChatContext context;

    public ChatRequest() {
    }

    @JsonCreator
    public ChatRequest(
        @JsonProperty("messages") List<ChatMessage> messages,
        @JsonProperty("tools") List<ToolDefinition> tools,
        @JsonProperty("context") ChatContext context) {
      this.messages = messages;
      this.tools = tools;
      this.context = context;
    }
  }

  public static class ChatContext {
    @JsonProperty
    public String currentSql;

    @JsonProperty
    public String currentSchema;

    @JsonProperty
    public List<String> availableSchemas;

    @JsonProperty
    public String error;

    @JsonProperty
    public ResultSummary resultSummary;

    @JsonProperty
    public boolean logAnalysisMode;

    @JsonProperty
    public String logFileName;

    @JsonProperty
    public List<String> logLines;

    public ChatContext() {
    }

    @JsonCreator
    public ChatContext(
        @JsonProperty("currentSql") String currentSql,
        @JsonProperty("currentSchema") String currentSchema,
        @JsonProperty("availableSchemas") List<String> availableSchemas,
        @JsonProperty("error") String error,
        @JsonProperty("resultSummary") ResultSummary resultSummary,
        @JsonProperty("logAnalysisMode") boolean logAnalysisMode,
        @JsonProperty("logFileName") String logFileName,
        @JsonProperty("logLines") List<String> logLines) {
      this.currentSql = currentSql;
      this.currentSchema = currentSchema;
      this.availableSchemas = availableSchemas;
      this.error = error;
      this.resultSummary = resultSummary;
      this.logAnalysisMode = logAnalysisMode;
      this.logFileName = logFileName;
      this.logLines = logLines;
    }
  }

  public static class ResultSummary {
    @JsonProperty
    public int rowCount;

    @JsonProperty
    public List<String> columns;

    @JsonProperty
    public List<String> columnTypes;

    public ResultSummary() {
    }

    @JsonCreator
    public ResultSummary(
        @JsonProperty("rowCount") int rowCount,
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("columnTypes") List<String> columnTypes) {
      this.rowCount = rowCount;
      this.columns = columns;
      this.columnTypes = columnTypes;
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
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get Prospector status",
      description = "Returns whether Prospector is enabled and configured")
  public AiStatusResponse getStatus() {
    try {
      LlmConfig config = getConfig();
      if (config == null) {
        return new AiStatusResponse(false, false);
      }
      boolean configured = config.getApiKey() != null && !config.getApiKey().isEmpty()
          && config.getModel() != null && !config.getModel().isEmpty();
      return new AiStatusResponse(config.isEnabled() && configured, configured);
    } catch (Exception e) {
      logger.error("Error checking AI status", e);
      return new AiStatusResponse(false, false);
    }
  }

  @POST
  @Path("/chat")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces("text/event-stream")
  @Operation(summary = "Stream AI chat completion",
      description = "Sends messages to the LLM and streams back SSE events")
  public Response chat(ChatRequest request) {
    try {
      LlmConfig config = getConfig();
      if (config == null || !config.isEnabled()) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
            .entity(new ErrorResponse("Prospector is not enabled"))
            .type(MediaType.APPLICATION_JSON)
            .build();
      }

      LlmProvider provider = LlmProviderRegistry.get(config.getProvider());
      if (provider == null) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Unknown LLM provider: " + config.getProvider()))
            .type(MediaType.APPLICATION_JSON)
            .build();
      }

      // Build the full message list with system prompt
      List<ChatMessage> fullMessages = buildMessages(config, request);
      List<ToolDefinition> tools = request.tools != null ? request.tools : new ArrayList<>();

      StreamingOutput stream = out -> {
        try {
          provider.streamChatCompletion(config, fullMessages, tools, out);
        } catch (Exception e) {
          logger.error("Error during AI chat streaming", e);
          try {
            ObjectMapper mapper = new ObjectMapper();
            String errorData = "{\"message\":" + mapper.writeValueAsString(e.getMessage()) + "}";
            String sse = "event: error\ndata: " + errorData + "\n\n";
            out.write(sse.getBytes("UTF-8"));
            out.flush();
          } catch (Exception writeErr) {
            logger.error("Error writing error event", writeErr);
          }
        }
      };

      return Response.ok(stream)
          .header("Content-Type", "text/event-stream")
          .header("Cache-Control", "no-cache")
          .header("Connection", "keep-alive")
          .header("X-Accel-Buffering", "no")
          .build();
    } catch (Exception e) {
      logger.error("Error initiating AI chat", e);
      return Response.serverError()
          .entity(new ErrorResponse("Failed to initiate chat: " + e.getMessage()))
          .type(MediaType.APPLICATION_JSON)
          .build();
    }
  }

  // ==================== Helper Methods ====================

  private List<ChatMessage> buildMessages(LlmConfig config, ChatRequest request) {
    List<ChatMessage> messages = new ArrayList<>();

    // Build system prompt
    StringBuilder systemPrompt = new StringBuilder();
    systemPrompt.append("You are an AI assistant for Apache Drill SQL Lab. ");
    systemPrompt.append("Apache Drill is a schema-free SQL query engine that supports ");
    systemPrompt.append("querying various data sources (JSON, CSV, Parquet, databases, APIs) ");
    systemPrompt.append("using ANSI SQL.\n\n");

    // Add context
    if (request.context != null) {
      ChatContext ctx = request.context;

      if (ctx.currentSchema != null && !ctx.currentSchema.isEmpty()) {
        systemPrompt.append("Current schema: ").append(ctx.currentSchema).append("\n");
      }

      if (ctx.availableSchemas != null && !ctx.availableSchemas.isEmpty()) {
        systemPrompt.append("Available schemas: ")
            .append(String.join(", ", ctx.availableSchemas)).append("\n");
      }

      if (ctx.currentSql != null && !ctx.currentSql.isEmpty()) {
        systemPrompt.append("\nCurrent SQL in editor:\n```sql\n")
            .append(ctx.currentSql).append("\n```\n");
      }

      if (ctx.error != null && !ctx.error.isEmpty()) {
        systemPrompt.append("\nCurrent error: ").append(ctx.error).append("\n");
      }

      if (ctx.logAnalysisMode) {
        systemPrompt.append("\nYou are also a log analysis expert for Apache Drill. ");
        systemPrompt.append("When analyzing log lines:\n");
        systemPrompt.append("- Identify error patterns and their root causes\n");
        systemPrompt.append("- Explain what each log level (ERROR, WARN, INFO, DEBUG) means in context\n");
        systemPrompt.append("- Suggest configuration changes or actions to resolve issues\n");
        systemPrompt.append("- Identify performance concerns from thread contention or slow operations\n");
        systemPrompt.append("- Reference Drill documentation when relevant\n");
        systemPrompt.append("- Users can query log files via SQL using the dfs.logs workspace\n\n");

        if (ctx.logFileName != null && !ctx.logFileName.isEmpty()) {
          systemPrompt.append("Log file being analyzed: ").append(ctx.logFileName).append("\n");
        }

        if (ctx.logLines != null && !ctx.logLines.isEmpty()) {
          int maxContextLines = Math.min(ctx.logLines.size(), 200);
          systemPrompt.append("\nLog lines for context:\n```\n");
          for (int i = 0; i < maxContextLines; i++) {
            systemPrompt.append(ctx.logLines.get(i)).append("\n");
          }
          if (ctx.logLines.size() > maxContextLines) {
            systemPrompt.append("... (").append(ctx.logLines.size() - maxContextLines)
                .append(" more lines truncated)\n");
          }
          systemPrompt.append("```\n");
        }
      }

      if (ctx.resultSummary != null) {
        ResultSummary rs = ctx.resultSummary;
        systemPrompt.append("\nQuery results: ").append(rs.rowCount).append(" rows");
        if (rs.columns != null && !rs.columns.isEmpty()) {
          systemPrompt.append(", columns: ");
          for (int i = 0; i < rs.columns.size(); i++) {
            if (i > 0) {
              systemPrompt.append(", ");
            }
            systemPrompt.append(rs.columns.get(i));
            if (rs.columnTypes != null && i < rs.columnTypes.size()) {
              systemPrompt.append(" (").append(rs.columnTypes.get(i)).append(")");
            }
          }
        }
        systemPrompt.append("\n");
      }
    }

    systemPrompt.append("\nWhen generating SQL, use Apache Drill SQL syntax. ");
    systemPrompt.append("Use backtick quoting for identifiers with special characters. ");
    systemPrompt.append("Use `LIMIT` for row limiting.\n\n");
    systemPrompt.append("You have tools available to execute SQL, explore schemas, ");
    systemPrompt.append("create visualizations and dashboards. Use them proactively to help the user.");

    // Append custom system prompt if configured
    if (config.getSystemPrompt() != null && !config.getSystemPrompt().isEmpty()) {
      systemPrompt.append("\n\n").append(config.getSystemPrompt());
    }

    messages.add(ChatMessage.system(systemPrompt.toString()));

    // Add user messages
    if (request.messages != null) {
      messages.addAll(request.messages);
    }

    return messages;
  }

  private LlmConfig getConfig() {
    try {
      PersistentStore<LlmConfig> store = getStore();
      return store.get(CONFIG_KEY);
    } catch (Exception e) {
      logger.error("Error reading AI config", e);
      return null;
    }
  }

  private PersistentStore<LlmConfig> getStore() {
    if (cachedStore == null) {
      synchronized (ProspectorResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    LlmConfig.class
                )
                .name(CONFIG_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new RuntimeException("Failed to access AI config store", e);
          }
        }
      }
    }
    return cachedStore;
  }
}
