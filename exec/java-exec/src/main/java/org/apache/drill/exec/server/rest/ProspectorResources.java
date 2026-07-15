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
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.ai.AiEvent;
import org.apache.drill.exec.server.rest.ai.AiEventLogger;
import org.apache.drill.exec.server.rest.ai.AiPricing;
import org.apache.drill.exec.server.rest.ai.ChatMessage;
import org.apache.drill.exec.server.rest.ai.LlmCallResult;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.apache.drill.exec.server.rest.ai.ToolCall;
import org.apache.drill.exec.server.rest.ai.LlmProvider;
import org.apache.drill.exec.server.rest.ai.LlmProviderRegistry;
import org.apache.drill.exec.server.rest.ai.ToolDefinition;
import org.apache.drill.exec.server.rest.ai.UsageObserver;
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
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.StreamingOutput;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  private static final String DEFAULT_FEATURE = "prospector_chat";
  private static final String PROJECTS_STORE_NAME = "drill.sqllab.projects";
  private static final String SAVED_QUERIES_STORE_NAME = "drill.sqllab.saved_queries";
  private static final int PROJECT_BLOCK_MAX_CHARS = 2000;
  private static final int SAVED_QUERY_SQL_MAX_CHARS = 500;

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<LlmConfig> cachedStore;
  private static volatile PersistentStore<ProjectResources.Project> cachedProjectStore;
  private static volatile PersistentStore<SavedQueryResources.SavedQuery> cachedSavedQueryStore;

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

  /**
   * Context the browser attaches to a chat call. Every field is an optional
   * enhancement, so an unknown one is ignored rather than rejected: the browser may
   * be running newer assets than the Drillbit (or a cached older build), and losing
   * one hint is survivable where failing the whole chat is not.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ChatContext {
    /** Active project, when the call originates inside one. */
    @JsonProperty
    public String projectId;

    /** True when the user is working in the notebook tab. */
    @JsonProperty
    public boolean notebookMode;

    /** Name of the DataFrame variable available in the notebook. */
    @JsonProperty
    public String notebookDfName;

    /** DataFrame shape, e.g. "10 rows x 3 cols". */
    @JsonProperty
    public String notebookDfShape;

    /** Column names available on the notebook DataFrame. */
    @JsonProperty
    public List<String> notebookColumns;

    /** Code in the cell the user is currently working on. */
    @JsonProperty
    public String notebookCellCode;

    /** Error from the last cell execution, if any. */
    @JsonProperty
    public String notebookCellError;

    @JsonProperty
    public String currentSql;

    /** Which UI feature originated this call; used for analytics attribution. */
    @JsonProperty
    public String feature;

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

    @JsonProperty
    public boolean dashboardSummaryMode;

    @JsonProperty
    public List<DashboardDataContext> dashboardData;

    @JsonProperty
    public boolean dashboardQnAMode;

    @JsonProperty
    public boolean dashboardNlFilterMode;

    @JsonProperty
    public boolean dashboardAlertMode;

    @JsonProperty
    public String dashboardTone;

    @JsonProperty
    public boolean dashboardAnomalyFocus;

    @JsonProperty
    public String previousSummary;

    @JsonProperty
    public List<ProjectDatasetRef> projectDatasets;

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
        @JsonProperty("logLines") List<String> logLines,
        @JsonProperty("dashboardSummaryMode") boolean dashboardSummaryMode,
        @JsonProperty("dashboardData") List<DashboardDataContext> dashboardData,
        @JsonProperty("dashboardQnAMode") boolean dashboardQnAMode,
        @JsonProperty("dashboardNlFilterMode") boolean dashboardNlFilterMode,
        @JsonProperty("dashboardAlertMode") boolean dashboardAlertMode,
        @JsonProperty("dashboardTone") String dashboardTone,
        @JsonProperty("dashboardAnomalyFocus") boolean dashboardAnomalyFocus,
        @JsonProperty("previousSummary") String previousSummary,
        @JsonProperty("projectDatasets") List<ProjectDatasetRef> projectDatasets) {
      this.currentSql = currentSql;
      this.currentSchema = currentSchema;
      this.availableSchemas = availableSchemas;
      this.error = error;
      this.resultSummary = resultSummary;
      this.logAnalysisMode = logAnalysisMode;
      this.logFileName = logFileName;
      this.logLines = logLines;
      this.dashboardSummaryMode = dashboardSummaryMode;
      this.dashboardData = dashboardData;
      this.dashboardQnAMode = dashboardQnAMode;
      this.dashboardNlFilterMode = dashboardNlFilterMode;
      this.dashboardAlertMode = dashboardAlertMode;
      this.dashboardTone = dashboardTone;
      this.dashboardAnomalyFocus = dashboardAnomalyFocus;
      this.previousSummary = previousSummary;
      this.projectDatasets = projectDatasets;
    }
  }

  public static class ProjectDatasetRef {
    @JsonProperty
    public String type;

    @JsonProperty
    public String schema;

    @JsonProperty
    public String table;

    @JsonProperty
    public String label;

    public ProjectDatasetRef() {
    }

    @JsonCreator
    public ProjectDatasetRef(
        @JsonProperty("type") String type,
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("label") String label) {
      this.type = type;
      this.schema = schema;
      this.table = table;
      this.label = label;
    }
  }

  public static class DashboardDataContext {
    @JsonProperty
    public String panelName;

    @JsonProperty
    public String sql;

    @JsonProperty
    public List<String> columns;

    @JsonProperty
    public List<String> columnTypes;

    @JsonProperty
    public int rowCount;

    @JsonProperty
    public List<Map<String, Object>> sampleRows;

    public DashboardDataContext() {
    }

    @JsonCreator
    public DashboardDataContext(
        @JsonProperty("panelName") String panelName,
        @JsonProperty("sql") String sql,
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("columnTypes") List<String> columnTypes,
        @JsonProperty("rowCount") int rowCount,
        @JsonProperty("sampleRows") List<Map<String, Object>> sampleRows) {
      this.panelName = panelName;
      this.sql = sql;
      this.columns = columns;
      this.columnTypes = columnTypes;
      this.rowCount = rowCount;
      this.sampleRows = sampleRows;
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
  public Response chat(ChatRequest request, @Context SecurityContext sc) {
    String username = resolveUser(sc);
    String feature = request != null && request.context != null ? request.context.feature : null;
    // Declared outside the try so the outer catch can still attribute provider/model
    // when the config loaded fine and a later step (buildMessages, pricing) threw.
    LlmConfig config = null;
    try {
      config = getConfig();
      if (config == null || !config.isEnabled()) {
        recordSetupFailure(config, username, feature,
            "ProspectorDisabled", "Prospector is not enabled");
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
            .entity(new ErrorResponse("Prospector is not enabled"))
            .type(MediaType.APPLICATION_JSON)
            .build();
      }

      LlmProvider provider = LlmProviderRegistry.get(config.getProvider());
      if (provider == null) {
        recordSetupFailure(config, username, feature,
            "UnknownProvider", "Unknown LLM provider: " + config.getProvider());
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Unknown LLM provider: " + config.getProvider()))
            .type(MediaType.APPLICATION_JSON)
            .build();
      }

      // Build the full message list with system prompt
      List<ChatMessage> fullMessages = buildMessages(config, request);
      List<ToolDefinition> tools = request.tools != null ? request.tools : new ArrayList<>();

      String userMessage = lastUserMessage(request);
      String fullPrompt = renderFullPrompt(fullMessages);
      LlmConfig snapshot = config;

      // Pricing for the configured model — looked up once per call. Used to
      // enrich usage SSE events with a server-computed cost so the client can
      // render a live "tokens · $cost" pill without exposing pricing config.
      AiPricing pricing = AiPricingResources
          .snapshot(workManager, storeProvider)
          .get(AiPricing.key(snapshot.getProvider(), snapshot.getModel()));

      StreamingOutput stream = out -> {
        long start = System.currentTimeMillis();
        LlmCallResult callResult = null;
        Exception failure = null;
        UsageObserver observer = (in, outTokens) -> emitUsageEvent(out, pricing, in, outTokens);
        try {
          callResult = provider.streamChatCompletion(snapshot, fullMessages, tools, out, observer);
        } catch (Exception e) {
          failure = e;
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
        } finally {
          recordEvent(snapshot, username, feature, userMessage, fullPrompt,
              callResult, failure, System.currentTimeMillis() - start);
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
      recordSetupFailure(config, username, feature, e.getClass().getSimpleName(), e.getMessage());
      return Response.serverError()
          .entity(new ErrorResponse("Failed to initiate chat: " + e.getMessage()))
          .type(MediaType.APPLICATION_JSON)
          .build();
    }
  }

  /**
   * Writes a normalized "usage" SSE event with cumulative token counts and a
   * server-computed cost (when pricing is configured for the active model).
   * Best-effort — IO errors during emission are swallowed since the client may
   * have already disconnected, which is not a chat failure.
   */
  private static void emitUsageEvent(java.io.OutputStream out, AiPricing pricing,
      Integer promptTokens, Integer responseTokens) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      java.util.Map<String, Object> payload = new java.util.LinkedHashMap<>();
      if (promptTokens != null) {
        payload.put("promptTokens", promptTokens);
      }
      if (responseTokens != null) {
        payload.put("responseTokens", responseTokens);
      }
      int total = (promptTokens != null ? promptTokens : 0)
          + (responseTokens != null ? responseTokens : 0);
      payload.put("totalTokens", total);
      if (pricing != null) {
        double inputCost = ((promptTokens != null ? promptTokens : 0) / 1_000_000.0)
            * pricing.getInputPricePerMTokens();
        double outputCost = ((responseTokens != null ? responseTokens : 0) / 1_000_000.0)
            * pricing.getOutputPricePerMTokens();
        payload.put("costUsd", round4(inputCost + outputCost));
        payload.put("currency", pricing.getCurrency());
      }
      String sse = "event: usage\ndata: " + mapper.writeValueAsString(payload) + "\n\n";
      out.write(sse.getBytes("UTF-8"));
      out.flush();
    } catch (Exception ignored) {
      // Stream may already be closed by the client — non-fatal.
    }
  }

  private static double round4(double v) {
    return Math.round(v * 10000.0) / 10000.0;
  }

  /**
   * Resolves the authenticated username for AI analytics events, falling back to
   * "anonymous" when there is no principal (e.g. auth disabled) so that dashboard
   * grouping by user is never broken by a null value.
   */
  static String resolveUser(SecurityContext sc) {
    if (sc == null) {
      return "anonymous";
    }
    Principal p = sc.getUserPrincipal();
    if (p == null || p.getName() == null || p.getName().isEmpty()) {
      return "anonymous";
    }
    return p.getName();
  }

  private static String lastUserMessage(ChatRequest request) {
    if (request == null || request.messages == null) {
      return null;
    }
    for (int i = request.messages.size() - 1; i >= 0; i--) {
      ChatMessage m = request.messages.get(i);
      if ("user".equals(m.getRole()) && m.getContent() != null) {
        return m.getContent();
      }
    }
    return null;
  }

  static String renderFullPrompt(List<ChatMessage> messages) {
    if (messages == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (ChatMessage m : messages) {
      sb.append("[").append(m.getRole()).append("]\n");
      if (m.getContent() != null) {
        sb.append(m.getContent());
      }
      // An assistant's tool calls live in toolCalls, not content. Rendering only
      // content drops every tool invocation from the recorded prompt, which hides
      // exactly the behaviour the analytics log is consulted to explain.
      if (m.getToolCalls() != null) {
        for (ToolCall tc : m.getToolCalls()) {
          sb.append("[tool_call] ").append(tc.getName())
              .append("(").append(tc.getArguments()).append(")\n");
        }
      }
      sb.append("\n\n");
    }
    return sb.toString();
  }

  private static void recordEvent(LlmConfig config, String username, String feature,
      String userMessage, String fullPrompt, LlmCallResult callResult, Exception failure,
      long durationMs) {
    try {
      AiEventLogger.log(buildEvent(config, username, feature, userMessage, fullPrompt,
          callResult, failure, durationMs));
    } catch (Exception logErr) {
      logger.warn("Failed to record AI usage event", logErr);
    }
  }

  /**
   * Builds a token-free event for an AI interaction whose outcome is already decided —
   * a setup failure that never reached the provider, or a non-streaming call such as
   * the admin config test that reports its own success. Package-private and side-effect
   * free so the recorded fields can be asserted directly.
   *
   * <p>Success semantics for most recorded events live here and in {@link #buildEvent};
   * callers should prefer these builders over hand-rolling AiEvent construction, since
   * "no exception thrown" is not generally the same as "the call succeeded". A caller
   * may still hand-roll construction when its own success signal is a thrown exception
   * rather than an error-carrying result — see TranspileResources.logTranspile.
   */
  static AiEvent buildOutcomeEvent(LlmConfig config, String username, String feature,
      boolean success, String errorClass, String message, long durationMs) {
    AiEvent event = buildEvent(config, username, feature, null, null, null, null, durationMs);
    event.success = success;
    event.cancelled = false;
    event.errorClass = success ? null : errorClass;
    event.error = success ? null : message;
    return event;
  }

  /**
   * Builds the event for a failure that never reached the provider. Package-private
   * and side-effect free so the recorded fields can be asserted directly.
   */
  static AiEvent buildSetupFailureEvent(LlmConfig config, String username, String feature,
      String errorClass, String message) {
    return buildOutcomeEvent(config, username, feature, false, errorClass, message, 0L);
  }

  /** Records a setup failure that never reached the provider. */
  private static void recordSetupFailure(LlmConfig config, String username, String feature,
      String errorClass, String message) {
    try {
      AiEventLogger.log(buildSetupFailureEvent(config, username, feature, errorClass, message));
    } catch (Exception logErr) {
      logger.warn("Failed to record AI setup failure", logErr);
    }
  }

  /**
   * Builds the event for a single LLM call. Package-private and side-effect free
   * so the recorded fields can be asserted directly.
   */
  static AiEvent buildEvent(LlmConfig config, String username, String feature,
      String userMessage, String fullPrompt, LlmCallResult callResult, Exception failure,
      long durationMs) {
    AiEvent event = new AiEvent();
    event.ts = AiEventLogger.nowIso();
    event.user = username;
    event.feature = (feature == null || feature.trim().isEmpty()) ? DEFAULT_FEATURE : feature;
    event.source = "server";
    event.provider = config != null ? config.getProvider() : null;
    event.model = config != null ? config.getModel() : null;
    event.durationMs = durationMs;
    // A missing callResult means no completed call, even when nothing was thrown:
    // an Error escapes the streaming catch(Exception) but still runs its finally.
    event.success = failure == null && callResult != null;
    event.cancelled = AiEventLogger.isClientCancellation(failure);
    if (failure != null) {
      event.errorClass = event.cancelled
          ? "ClientCancelled"
          : failure.getClass().getSimpleName();
      event.error = failure.getMessage();
    }
    if (callResult != null) {
      event.promptTokens = callResult.getPromptTokens();
      event.responseTokens = callResult.getResponseTokens();
      event.totalTokens = callResult.getTotalTokens();
      event.response = AiEventLogger.truncate(callResult.getResponseText());
    }
    event.userMessage = AiEventLogger.truncate(userMessage);
    event.prompt = AiEventLogger.truncate(fullPrompt);
    return event;
  }

  // ==================== Helper Methods ====================

  List<ChatMessage> buildMessages(LlmConfig config, ChatRequest request) {
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

      if (ctx.projectDatasets != null && !ctx.projectDatasets.isEmpty()) {
        systemPrompt.append("\nYou are working within a PROJECT scope. ");
        systemPrompt.append("ONLY use these datasets — do NOT explore outside them:\n");
        for (ProjectDatasetRef ds : ctx.projectDatasets) {
          systemPrompt.append("- ").append(ds.label);
          if (ds.schema != null) {
            systemPrompt.append(" (schema: ").append(ds.schema);
            if (ds.table != null) {
              systemPrompt.append(", table: ").append(ds.table);
            }
            systemPrompt.append(")");
          }
          systemPrompt.append("\n");
        }
        systemPrompt.append("\n");
      } else if (ctx.availableSchemas != null && !ctx.availableSchemas.isEmpty()) {
        systemPrompt.append("Available schemas: ")
            .append(String.join(", ", ctx.availableSchemas)).append("\n");
      }

      if (ctx.projectId != null && !ctx.projectId.trim().isEmpty()) {
        ProjectResources.Project project = loadProject(ctx.projectId);
        if (project != null) {
          systemPrompt.append(buildProjectBlock(project,
              loadSavedQueries(project.getSavedQueryIds())));
        }
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

      if (ctx.dashboardSummaryMode) {
        systemPrompt.append("\nYou are generating an executive summary for a dashboard. ");
        systemPrompt.append("Analyze the data from all dashboard panels and provide:\n");
        systemPrompt.append("- Key insights and trends\n");
        systemPrompt.append("- Anomalies or areas needing attention\n");
        systemPrompt.append("- Status indicators using emoji (✅ good, ⚠️ warning, ❌ critical)\n");
        systemPrompt.append("- Clear, well-formatted sections using markdown\n");
        systemPrompt.append("- You may use markdown images ![alt](url) if helpful\n\n");

        appendDashboardData(systemPrompt, ctx);
      }

      if (ctx.dashboardQnAMode) {
        systemPrompt.append("\nYou are answering questions about dashboard data. ");
        systemPrompt.append("IMPORTANT: Do NOT suggest running queries, retrieving data, or using tools. ");
        systemPrompt.append("You have all the information needed in the dashboard data below. ");
        systemPrompt.append("Answer questions directly and concisely (1-2 sentences) using ONLY the data provided.\n\n");

        appendDashboardData(systemPrompt, ctx);
      }

      if (ctx.dashboardNlFilterMode) {
        systemPrompt.append("\nReturn ONLY a JSON array of filter objects. ");
        systemPrompt.append("Each filter object should have these fields: ");
        systemPrompt.append("column (string), value (string), isTemporal (boolean), ");
        systemPrompt.append("isNumeric (boolean), numericOp (string: =, !=, >, >=, <, <=, between), ");
        systemPrompt.append("rangeStart (string, ISO date), rangeEnd (string, ISO date). ");
        systemPrompt.append("Do not include any explanation, only the JSON array.\n\n");

        if (ctx.dashboardData != null && !ctx.dashboardData.isEmpty()) {
          systemPrompt.append("Available columns:\n");
          for (DashboardDataContext ddc : ctx.dashboardData) {
            if (ddc.columns != null) {
              for (int j = 0; j < ddc.columns.size(); j++) {
                systemPrompt.append("- ").append(ddc.columns.get(j));
                if (ddc.columnTypes != null && j < ddc.columnTypes.size()) {
                  systemPrompt.append(" (").append(ddc.columnTypes.get(j)).append(")");
                }
                systemPrompt.append(" from \"").append(ddc.panelName).append("\"\n");
              }
            }
          }
        }
      }

      if (ctx.dashboardAlertMode) {
        systemPrompt.append("\nYou are analyzing triggered dashboard alert conditions. ");
        systemPrompt.append("Explain what the alert conditions mean in context, ");
        systemPrompt.append("why they may have triggered, and suggest actions to address them.\n\n");

        appendDashboardData(systemPrompt, ctx);
      }

      // Tone instruction
      if (ctx.dashboardTone != null && !ctx.dashboardTone.isEmpty()) {
        switch (ctx.dashboardTone) {
          case "executive":
            systemPrompt.append("\nWrite in a concise, high-level executive style. ")
                .append("Focus on business impact and strategic implications.\n");
            break;
          case "technical":
            systemPrompt.append("\nWrite in a detailed technical style. ")
                .append("Include specific metrics, data points, and technical context.\n");
            break;
          case "casual":
            systemPrompt.append("\nWrite in a friendly, conversational tone. ")
                .append("Keep it approachable and easy to understand.\n");
            break;
          default:
            break;
        }
      }

      // Anomaly focus
      if (ctx.dashboardAnomalyFocus) {
        systemPrompt.append("\nPay special attention to anomalies, outliers, and ")
            .append("unexpected patterns in the data. Highlight any values that deviate ")
            .append("significantly from expected ranges or historical norms.\n");
      }

      // Historical comparison
      if (ctx.previousSummary != null && !ctx.previousSummary.isEmpty()) {
        systemPrompt.append("\nHere is the previous summary for comparison. ")
            .append("Note any changes or trends:\n---\nPREVIOUS SUMMARY:\n")
            .append(ctx.previousSummary).append("\n---\n");
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

    systemPrompt.append("IMPORTANT: You have tools available and MUST use them proactively:\n");
    systemPrompt.append("- When a user asks a data question, ALWAYS use list_schemas and ");
    systemPrompt.append("get_schema_info to discover available data BEFORE writing SQL.\n");
    systemPrompt.append("- Use list_schemas to see all available schemas/data sources.\n");
    systemPrompt.append("- Drill uses hierarchical schemas (e.g., 'mysql' plugin has sub-schemas ");
    systemPrompt.append("like 'mysql.store'). Use get_schema_info on sub-schemas to find tables.\n");
    systemPrompt.append("- Use get_schema_info with a table name to discover columns before writing queries.\n");
    systemPrompt.append("- NEVER ask the user for schema or table names — explore and find them yourself.\n");
    systemPrompt.append("- After discovering the schema, write and execute the SQL query.\n");
    systemPrompt.append("- You can also create visualizations, dashboards, and save queries.\n");

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

  private void appendDashboardData(StringBuilder systemPrompt, ChatContext ctx) {
    if (ctx.dashboardData != null && !ctx.dashboardData.isEmpty()) {
      systemPrompt.append("Dashboard panels data:\n\n");
      for (int i = 0; i < ctx.dashboardData.size(); i++) {
        DashboardDataContext ddc = ctx.dashboardData.get(i);
        systemPrompt.append("### Panel ").append(i + 1).append(": ")
            .append(ddc.panelName).append("\n");
        if (ddc.sql != null) {
          systemPrompt.append("SQL: `").append(ddc.sql).append("`\n");
        }
        if (ddc.columns != null) {
          systemPrompt.append("Columns: ");
          for (int j = 0; j < ddc.columns.size(); j++) {
            if (j > 0) {
              systemPrompt.append(", ");
            }
            systemPrompt.append(ddc.columns.get(j));
            if (ddc.columnTypes != null && j < ddc.columnTypes.size()) {
              systemPrompt.append(" (").append(ddc.columnTypes.get(j)).append(")");
            }
          }
          systemPrompt.append("\n");
        }
        systemPrompt.append("Row count: ").append(ddc.rowCount).append("\n");
        if (ddc.sampleRows != null && !ddc.sampleRows.isEmpty()) {
          systemPrompt.append("Sample data:\n```json\n");
          try {
            ObjectMapper mapper = new ObjectMapper();
            systemPrompt.append(mapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(ddc.sampleRows));
          } catch (Exception e) {
            systemPrompt.append("[Error serializing sample data]");
          }
          systemPrompt.append("\n```\n");
        }
        systemPrompt.append("\n");
      }
    }
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

  private PersistentStore<ProjectResources.Project> getProjectStore() {
    if (cachedProjectStore == null) {
      synchronized (ProspectorResources.class) {
        if (cachedProjectStore == null) {
          try {
            cachedProjectStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    ProjectResources.Project.class
                )
                .name(PROJECTS_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new RuntimeException("Failed to access projects store", e);
          }
        }
      }
    }
    return cachedProjectStore;
  }

  private PersistentStore<SavedQueryResources.SavedQuery> getSavedQueryStore() {
    if (cachedSavedQueryStore == null) {
      synchronized (ProspectorResources.class) {
        if (cachedSavedQueryStore == null) {
          try {
            cachedSavedQueryStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    SavedQueryResources.SavedQuery.class
                )
                .name(SAVED_QUERIES_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new RuntimeException("Failed to access saved queries store", e);
          }
        }
      }
    }
    return cachedSavedQueryStore;
  }

  private ProjectResources.Project loadProject(String projectId) {
    if (projectId == null || projectId.trim().isEmpty()) {
      return null;
    }
    try {
      return getProjectStore().get(projectId);
    } catch (Exception e) {
      // Context is an enhancement; never fail the chat because it is unavailable.
      logger.debug("Could not load project {} for AI context", projectId, e);
      return null;
    }
  }

  private List<SavedQueryResources.SavedQuery> loadSavedQueries(List<String> ids) {
    List<SavedQueryResources.SavedQuery> out = new ArrayList<>();
    if (ids == null || ids.isEmpty()) {
      return out;
    }
    try {
      PersistentStore<SavedQueryResources.SavedQuery> store = getSavedQueryStore();
      for (String id : ids) {
        SavedQueryResources.SavedQuery q = store.get(id);
        if (q != null) {
          out.add(q);
        }
      }
    } catch (Exception e) {
      logger.debug("Could not load saved queries for AI context", e);
    }
    return out;
  }

  /**
   * Renders the project's own descriptive metadata for the system prompt. Pure and
   * package-private so its output can be asserted without a store.
   *
   * <p>Wiki bodies are deliberately excluded — a page can be tens of KB and the whole
   * system prompt is re-sent on every tool round, so bodies are fetched on demand by
   * the get_project_docs tool instead.
   */
  static String buildProjectBlock(ProjectResources.Project project,
      List<SavedQueryResources.SavedQuery> savedQueries) {
    if (project == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("\nPROJECT CONTEXT — the user's own description of this work:\n");
    sb.append("Project: ").append(project.getName()).append("\n");
    if (project.getDescription() != null && !project.getDescription().trim().isEmpty()) {
      sb.append("Description: ").append(project.getDescription()).append("\n");
    }
    if (project.getTags() != null && !project.getTags().isEmpty()) {
      sb.append("Tags: ").append(String.join(", ", project.getTags())).append("\n");
    }
    if (savedQueries != null && !savedQueries.isEmpty()) {
      sb.append("\nSaved queries in this project:\n");
      for (SavedQueryResources.SavedQuery q : savedQueries) {
        sb.append("- ").append(q.getName());
        if (q.getDescription() != null && !q.getDescription().trim().isEmpty()) {
          sb.append(": ").append(q.getDescription());
        }
        if (q.getSql() != null) {
          sb.append("\n  SQL: ").append(truncate(q.getSql(), SAVED_QUERY_SQL_MAX_CHARS));
        }
        sb.append("\n");
      }
    }
    if (project.getWikiPages() != null && !project.getWikiPages().isEmpty()) {
      sb.append("\nProject documentation pages (call get_project_docs with a title to "
          + "read one in full):\n");
      for (ProjectResources.WikiPage page : project.getWikiPages()) {
        sb.append("- ").append(page.getTitle()).append("\n");
      }
    }
    return truncate(sb.toString(), PROJECT_BLOCK_MAX_CHARS);
  }

  private static String truncate(String text, int maxChars) {
    if (text == null || text.length() <= maxChars) {
      return text;
    }
    return text.substring(0, maxChars) + "...[truncated]";
  }
}
