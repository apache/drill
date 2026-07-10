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
package org.apache.drill.exec.server.rest.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LLM provider for custom/enterprise APIs with non-standard request/response formats.
 *
 * Uses configurable request templates and response mappings to support arbitrary API formats.
 * Request templates support simple variable substitution: {model}, {temperature}, {maxTokens}, {messages}, etc.
 * Response mappings use JSONPath-like syntax to extract deltas, tool calls, and completion status.
 */
public class EnterpriseProvider implements LlmProvider {

  private static final Logger logger = LoggerFactory.getLogger(EnterpriseProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MediaType JSON_TYPE = MediaType.parse("application/json");

  @Override
  public String getId() {
    return "enterprise";
  }

  @Override
  public String getDisplayName() {
    return "Enterprise (Custom API)";
  }

  @Override
  public ValidationResult validateConfig(LlmConfig config) {
    if (config.getModel() == null || config.getModel().isEmpty()) {
      return ValidationResult.error("Model name is required");
    }

    if (config.getApiEndpoint() == null || config.getApiEndpoint().isEmpty()) {
      return ValidationResult.error("API endpoint is required");
    }

    if (config.getRequestTemplate() == null || config.getRequestTemplate().isEmpty()) {
      return ValidationResult.error("Request template is required for enterprise provider");
    }

    // Validate that request template is valid JSON
    try {
      MAPPER.readTree(config.getRequestTemplate());
    } catch (Exception e) {
      return ValidationResult.error("Invalid request template JSON: " + e.getMessage());
    }

    // Validate that response mapping is valid JSON (if provided)
    if (config.getResponseMapping() != null && !config.getResponseMapping().isEmpty()) {
      try {
        MAPPER.readTree(config.getResponseMapping());
      } catch (Exception e) {
        return ValidationResult.error("Invalid response mapping JSON: " + e.getMessage());
      }
    }

    // Send a real probe to the endpoint (using the configured template) so "Test
    // Connection" actually exercises DNS, TLS, proxy, and auth instead of only
    // checking that the templates parse. This is the same request path the live
    // chat uses, so a failure here surfaces the real enterprise-gateway error.
    String url = config.getApiEndpoint();
    String requestBody;
    try {
      requestBody = buildCustomRequest(config, List.of(ChatMessage.user("ping")), null);
    } catch (Exception e) {
      logger.warn("Enterprise config test could not build request from template", e);
      return ValidationResult.error("Failed to build request from template: " + e.getMessage(),
          LlmProvider.describeException(url, e));
    }

    Request.Builder reqBuilder = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestBody, JSON_TYPE))
        .addHeader("Content-Type", "application/json");
    addBearerToken(reqBuilder, config);

    try (Response response = HttpClientFactory.createClient(config)
        .newCall(reqBuilder.build()).execute()) {
      if (response.isSuccessful()) {
        return ValidationResult.ok("Connection successful");
      }
      String body = response.body() != null ? response.body().string() : "";
      String details = LlmProvider.describeHttpError(url, response.code(), body);
      logger.warn("Enterprise config test failed:\n{}", details);
      return ValidationResult.error("Enterprise API error " + response.code() + ": "
          + (body.isEmpty() ? "(empty response)" : body), details);
    } catch (Exception e) {
      logger.warn("Enterprise config test could not reach {}", url, e);
      return ValidationResult.error("Could not reach endpoint: " + e.getMessage(),
          LlmProvider.describeException(url, e));
    }
  }

  @Override
  public LlmCallResult streamChatCompletion(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools, OutputStream out, UsageObserver usageObserver)
      throws Exception {
    UsageObserver observer = usageObserver != null ? usageObserver : UsageObserver.NOOP;

    // Create HTTP client with enterprise configuration
    OkHttpClient httpClient = HttpClientFactory.createClient(config);

    String url = config.getApiEndpoint();
    if (url == null || url.isEmpty()) {
      writeSseEvent(out, "error", "{\"message\":\"API endpoint is not configured\"}");
      return new LlmCallResult();
    }

    // Build custom request using template and variable substitution
    String requestBody = buildCustomRequest(config, messages, tools);

    Request.Builder reqBuilder = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestBody, JSON_TYPE))
        .addHeader("Content-Type", "application/json");
    addBearerToken(reqBuilder, config);
    Request request = reqBuilder.build();

    LlmCallResult result = new LlmCallResult();
    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        String errorBody = "";
        ResponseBody body = response.body();
        if (body != null) {
          errorBody = body.string();
        }
        String errorMsg = "Enterprise API error " + response.code() + ": " + errorBody;
        logger.error(errorMsg);
        writeSseEvent(out, "error", "{\"message\":" + MAPPER.writeValueAsString(errorMsg) + "}");
        return result;
      }

      ResponseBody body = response.body();
      if (body == null) {
        writeSseEvent(out, "error", "{\"message\":\"Empty response from Enterprise API\"}");
        return result;
      }

      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(body.byteStream(), StandardCharsets.UTF_8))) {
        processCustomStream(reader, out, result, config);
      }
    }
    return result;
  }

  /**
   * Add {@code Authorization: Bearer <apiKey>} from the configured API key, unless the
   * user already supplied an Authorization header via custom headers (that wins, so
   * non-Bearer schemes still work). Enterprise gateways commonly expect a bearer
   * token; without this the API Key field would be silently ignored for this provider.
   */
  private void addBearerToken(Request.Builder builder, LlmConfig config) {
    String apiKey = config.getApiKey();
    if (apiKey == null || apiKey.isEmpty()) {
      return;
    }
    Map<String, String> custom = config.getCustomHeaders();
    if (custom != null) {
      for (String key : custom.keySet()) {
        if ("authorization".equalsIgnoreCase(key)) {
          return;
        }
      }
    }
    builder.addHeader("Authorization", "Bearer " + apiKey);
  }

  /**
   * Build the request body by substituting variables into the template.
   * Supports: {model}, {temperature}, {maxTokens}, {messages}, {tools}, {additionalParameters}
   */
  private String buildCustomRequest(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools) throws Exception {
    String template = config.getRequestTemplate();
    ObjectMapper mapper = new ObjectMapper();

    // Parse template as JSON
    JsonNode templateNode = mapper.readTree(template);
    ObjectNode request = (ObjectNode) templateNode;

    // Perform simple variable substitution at the root level
    // This handles cases where template is like: {"query": "{messages}", "model": "{model}"}

    Map<String, Object> substitutions = new HashMap<>();
    substitutions.put("model", config.getModel());
    substitutions.put("temperature", config.getTemperature());
    substitutions.put("maxTokens", config.getMaxTokens());
    substitutions.put("messages", buildMessagesArray(messages));
    if (tools != null && !tools.isEmpty()) {
      substitutions.put("tools", buildToolsArray(tools));
    }

    // Try to substitute variables in the template string before parsing
    // This handles templates with literal variable references
    String substitutedTemplate = template;
    for (Map.Entry<String, Object> entry : substitutions.entrySet()) {
      String placeholder = "{" + entry.getKey() + "}";
      if (substitutedTemplate.contains(placeholder)) {
        String jsonValue = mapper.writeValueAsString(entry.getValue());
        substitutedTemplate = substitutedTemplate.replace(placeholder, jsonValue);
      }
    }

    // Parse the substituted template
    ObjectNode finalRequest = (ObjectNode) mapper.readTree(substitutedTemplate);

    // Inject additional parameters at the root level
    if (config.getAdditionalParameters() != null) {
      for (Map.Entry<String, Object> entry : config.getAdditionalParameters().entrySet()) {
        finalRequest.set(entry.getKey(), mapper.valueToTree(entry.getValue()));
      }
    }

    return mapper.writeValueAsString(finalRequest);
  }

  /**
   * Build a JSON array of messages in OpenAI format.
   */
  private Object buildMessagesArray(List<ChatMessage> messages) {
    List<Map<String, Object>> result = new java.util.ArrayList<>();
    for (ChatMessage msg : messages) {
      Map<String, Object> msgMap = new HashMap<>();
      msgMap.put("role", msg.getRole());
      if (msg.getContent() != null) {
        msgMap.put("content", msg.getContent());
      }
      result.add(msgMap);
    }
    return result;
  }

  /**
   * Build a JSON array of tools in OpenAI format.
   */
  private Object buildToolsArray(List<ToolDefinition> tools) {
    List<Map<String, Object>> result = new java.util.ArrayList<>();
    for (ToolDefinition tool : tools) {
      Map<String, Object> toolMap = new HashMap<>();
      toolMap.put("type", "function");
      Map<String, Object> fnMap = new HashMap<>();
      fnMap.put("name", tool.getName());
      fnMap.put("description", tool.getDescription());
      fnMap.put("parameters", tool.getParameters());
      toolMap.put("function", fnMap);
      result.add(toolMap);
    }
    return result;
  }

  /**
   * Process the custom API stream response.
   *
   * If responseMapping is configured, use it to extract deltas.
   * Otherwise, assume the response is already in the standard delta format.
   *
   * Standard format expectations:
   * - Content deltas: {"type": "content", "content": "..."}
   * - Tool calls: {"type": "tool_call_start", "id": "...", "name": "..."}
   * - Tool deltas: {"type": "tool_call_delta", "id": "...", "arguments": "..."}
   * - Tool end: {"type": "tool_call_end", "id": "..."}
   * - Done: finish_reason "stop" or "tool_calls"
   */
  private void processCustomStream(BufferedReader reader, OutputStream out, LlmCallResult result,
      LlmConfig config) throws Exception {
    String line;
    String responseMapping = config.getResponseMapping();
    boolean doneEmitted = false;

    while ((line = reader.readLine()) != null) {
      line = line.trim();

      if (line.isEmpty()) {
        continue;
      }

      // Handle both "data: " prefixed SSE and plain JSON
      String data = line;
      if (line.startsWith("data: ")) {
        data = line.substring(6).trim();
      }

      if ("[DONE]".equals(data)) {
        if (!doneEmitted) {
          writeSseEvent(out, "done", "{\"finish_reason\":\"stop\"}");
        }
        return;
      }

      try {
        JsonNode chunk = MAPPER.readTree(data);

        // If response mapping is provided, extract normalized deltas from custom format
        if (responseMapping != null && !responseMapping.isEmpty()) {
          processWithMapping(chunk, out, result, responseMapping);
        } else {
          // Assume the response is already in normalized format
          processNormalizedResponse(chunk, out, result);
        }
      } catch (Exception e) {
        logger.warn("Error parsing enterprise API response: {}", data, e);
      }
    }

    if (!doneEmitted) {
      writeSseEvent(out, "done", "{\"finish_reason\":\"stop\"}");
    }
  }

  /**
   * Process response assuming it's already in normalized delta format.
   */
  private void processNormalizedResponse(JsonNode chunk, OutputStream out, LlmCallResult result)
      throws Exception {
    // Check if this is a delta event
    if (chunk.has("type")) {
      String type = chunk.get("type").asText();

      if ("content".equals(type) && chunk.has("content")) {
        String content = chunk.get("content").asText();
        if (!content.isEmpty()) {
          result.appendResponseText(content);
          writeSseEvent(out, "delta",
              "{\"type\":\"content\",\"content\":" + MAPPER.writeValueAsString(content) + "}");
        }
      } else if ("tool_call_start".equals(type)) {
        writeSseEvent(out, "delta", chunk.toString());
      } else if ("tool_call_delta".equals(type)) {
        writeSseEvent(out, "delta", chunk.toString());
      } else if ("tool_call_end".equals(type)) {
        writeSseEvent(out, "delta", chunk.toString());
      }
    }

    // Check for finish_reason
    if (chunk.has("finish_reason")) {
      String reason = chunk.get("finish_reason").asText();
      writeSseEvent(out, "done", "{\"finish_reason\":\"" + reason + "\"}");
    }
  }

  /**
   * Process response using custom response mapping.
   *
   * Response mapping is a JSON object with keys like:
   * {
   *   "contentPath": "$.data.text",
   *   "toolCallStartPath": "$.data.function_call",
   *   "donePath": "$.data.finish_reason"
   * }
   *
   * This is a simplified implementation using basic string-based path extraction.
   * For production use, consider adding a proper JSONPath library.
   */
  private void processWithMapping(JsonNode chunk, OutputStream out, LlmCallResult result,
      String responseMapping) throws Exception {
    JsonNode mappingNode = MAPPER.readTree(responseMapping);

    // Extract content using contentPath
    if (mappingNode.has("contentPath")) {
      String contentPath = mappingNode.get("contentPath").asText();
      String content = extractValueByPath(chunk, contentPath);
      if (content != null && !content.isEmpty()) {
        result.appendResponseText(content);
        writeSseEvent(out, "delta",
            "{\"type\":\"content\",\"content\":" + MAPPER.writeValueAsString(content) + "}");
      }
    }

    // Extract finish reason using donePath
    if (mappingNode.has("donePath")) {
      String donePath = mappingNode.get("donePath").asText();
      String reason = extractValueByPath(chunk, donePath);
      if (reason != null && !reason.isEmpty()) {
        writeSseEvent(out, "done", "{\"finish_reason\":\"" + reason + "\"}");
      }
    }

    // For tool calls, custom mapping is more complex
    // This is a simplified version that assumes tool call data is returned as-is
    if (mappingNode.has("toolCallPath")) {
      String toolCallPath = mappingNode.get("toolCallPath").asText();
      JsonNode toolCallNode = extractNodeByPath(chunk, toolCallPath);
      if (toolCallNode != null) {
        // Assume tool call node has standard structure
        writeSseEvent(out, "delta", toolCallNode.toString());
      }
    }
  }

  /**
   * Extract a string value from JSON using a simplified path syntax.
   * Supports paths like "$.data.text" or "data.text".
   * Returns null if path not found.
   */
  private String extractValueByPath(JsonNode root, String path) {
    JsonNode node = extractNodeByPath(root, path);
    if (node != null && node.isTextual()) {
      return node.asText();
    }
    return null;
  }

  /**
   * Extract a JsonNode from the root using a simplified path syntax.
   * Supports paths like "$.data.text", "data.text", or direct field names.
   */
  private JsonNode extractNodeByPath(JsonNode root, String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }

    // Remove leading "$." if present
    if (path.startsWith("$.")) {
      path = path.substring(2);
    }

    // Split path into parts and navigate
    String[] parts = path.split("\\.");
    JsonNode current = root;

    for (String part : parts) {
      if (current == null) {
        return null;
      }
      current = current.get(part);
    }

    return current;
  }

  private static void writeSseEvent(OutputStream out, String event, String data) throws Exception {
    String sse = "event: " + event + "\ndata: " + data + "\n\n";
    out.write(sse.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }
}
