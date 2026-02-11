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
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.concurrent.TimeUnit;

/**
 * LLM provider for the Anthropic Claude API.
 * Calls /v1/messages with stream:true, translates Anthropic SSE events
 * (message_start, content_block_delta, etc.) to the normalized format.
 */
public class AnthropicProvider implements LlmProvider {

  private static final Logger logger = LoggerFactory.getLogger(AnthropicProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MediaType JSON_TYPE = MediaType.parse("application/json");
  private static final String DEFAULT_ENDPOINT = "https://api.anthropic.com";
  private static final String ANTHROPIC_VERSION = "2023-06-01";

  private final OkHttpClient httpClient;

  public AnthropicProvider() {
    this.httpClient = new OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .build();
  }

  @Override
  public String getId() {
    return "anthropic";
  }

  @Override
  public String getDisplayName() {
    return "Anthropic Claude";
  }

  @Override
  public void streamChatCompletion(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools, OutputStream out) throws Exception {

    String endpoint = config.getApiEndpoint();
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = DEFAULT_ENDPOINT;
    }
    if (endpoint.endsWith("/")) {
      endpoint = endpoint.substring(0, endpoint.length() - 1);
    }
    String url = endpoint + "/v1/messages";

    ObjectNode requestBody = buildRequestBody(config, messages, tools);

    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestBody.toString(), JSON_TYPE))
        .addHeader("Content-Type", "application/json")
        .addHeader("x-api-key", config.getApiKey() != null ? config.getApiKey() : "")
        .addHeader("anthropic-version", ANTHROPIC_VERSION)
        .build();

    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        String errorBody = "";
        ResponseBody body = response.body();
        if (body != null) {
          errorBody = body.string();
        }
        String errorMsg = "Anthropic API error " + response.code() + ": " + errorBody;
        logger.error(errorMsg);
        writeSseEvent(out, "error", "{\"message\":" + MAPPER.writeValueAsString(errorMsg) + "}");
        return;
      }

      ResponseBody body = response.body();
      if (body == null) {
        writeSseEvent(out, "error", "{\"message\":\"Empty response from Anthropic API\"}");
        return;
      }

      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(body.byteStream(), StandardCharsets.UTF_8))) {
        processAnthropicStream(reader, out);
      }
    }
  }

  @Override
  public ValidationResult validateConfig(LlmConfig config) {
    if (config.getApiKey() == null || config.getApiKey().isEmpty()) {
      return ValidationResult.error("API key is required for Anthropic");
    }
    if (config.getModel() == null || config.getModel().isEmpty()) {
      return ValidationResult.error("Model name is required");
    }
    return ValidationResult.ok("Configuration is valid");
  }

  private ObjectNode buildRequestBody(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools) {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", config.getModel());
    body.put("max_tokens", config.getMaxTokens());
    body.put("temperature", config.getTemperature());
    body.put("stream", true);

    // Extract system message
    String systemContent = null;
    ArrayNode messagesArray = body.putArray("messages");

    for (ChatMessage msg : messages) {
      if ("system".equals(msg.getRole())) {
        systemContent = msg.getContent();
        continue;
      }

      ObjectNode msgNode = messagesArray.addObject();
      msgNode.put("role", msg.getRole());

      if ("tool".equals(msg.getRole())) {
        // Anthropic uses tool_result content blocks
        ArrayNode contentArray = msgNode.putArray("content");
        ObjectNode block = contentArray.addObject();
        block.put("type", "tool_result");
        block.put("tool_use_id", msg.getToolCallId());
        block.put("content", msg.getContent() != null ? msg.getContent() : "");
      } else if ("assistant".equals(msg.getRole()) && msg.getToolCalls() != null
          && !msg.getToolCalls().isEmpty()) {
        // Assistant message with tool calls
        ArrayNode contentArray = msgNode.putArray("content");

        if (msg.getContent() != null && !msg.getContent().isEmpty()) {
          ObjectNode textBlock = contentArray.addObject();
          textBlock.put("type", "text");
          textBlock.put("text", msg.getContent());
        }

        for (ToolCall tc : msg.getToolCalls()) {
          ObjectNode toolUseBlock = contentArray.addObject();
          toolUseBlock.put("type", "tool_use");
          toolUseBlock.put("id", tc.getId());
          toolUseBlock.put("name", tc.getName());
          try {
            toolUseBlock.set("input", MAPPER.readTree(tc.getArguments()));
          } catch (Exception e) {
            toolUseBlock.putObject("input");
          }
        }
      } else {
        msgNode.put("content", msg.getContent() != null ? msg.getContent() : "");
      }
    }

    if (systemContent != null) {
      body.put("system", systemContent);
    }

    // Tools
    if (tools != null && !tools.isEmpty()) {
      ArrayNode toolsArray = body.putArray("tools");
      for (ToolDefinition tool : tools) {
        ObjectNode toolNode = toolsArray.addObject();
        toolNode.put("name", tool.getName());
        toolNode.put("description", tool.getDescription());
        toolNode.set("input_schema", MAPPER.valueToTree(tool.getParameters()));
      }
    }

    return body;
  }

  private void processAnthropicStream(BufferedReader reader, OutputStream out) throws Exception {
    // Track content blocks by index
    Map<Integer, String> blockTypes = new HashMap<>();
    Map<Integer, String> toolUseIds = new HashMap<>();
    Map<Integer, String> toolUseNames = new HashMap<>();
    boolean hasToolUse = false;

    String line;
    while ((line = reader.readLine()) != null) {
      line = line.trim();

      if (line.isEmpty()) {
        continue;
      }

      if (!line.startsWith("data: ")) {
        continue;
      }

      String data = line.substring(6).trim();

      try {
        JsonNode event = MAPPER.readTree(data);
        String type = event.has("type") ? event.get("type").asText() : "";

        switch (type) {
          case "content_block_start":
            handleContentBlockStart(event, blockTypes, toolUseIds, toolUseNames, out);
            if (event.has("content_block")) {
              JsonNode block = event.get("content_block");
              if ("tool_use".equals(block.path("type").asText())) {
                hasToolUse = true;
              }
            }
            break;

          case "content_block_delta":
            handleContentBlockDelta(event, blockTypes, toolUseIds, out);
            break;

          case "content_block_stop":
            handleContentBlockStop(event, blockTypes, toolUseIds, out);
            break;

          case "message_delta":
            // Check for stop_reason
            JsonNode messageDelta = event.get("delta");
            if (messageDelta != null && messageDelta.has("stop_reason")) {
              String stopReason = messageDelta.get("stop_reason").asText();
              if ("tool_use".equals(stopReason) || hasToolUse) {
                writeSseEvent(out, "done", "{\"finish_reason\":\"tool_calls\"}");
              } else {
                writeSseEvent(out, "done", "{\"finish_reason\":\"stop\"}");
              }
              return;
            }
            break;

          case "message_stop":
            if (hasToolUse) {
              writeSseEvent(out, "done", "{\"finish_reason\":\"tool_calls\"}");
            } else {
              writeSseEvent(out, "done", "{\"finish_reason\":\"stop\"}");
            }
            return;

          case "error":
            JsonNode errorNode = event.get("error");
            String errorMsg = errorNode != null ? errorNode.toString() : "Unknown error";
            writeSseEvent(out, "error", "{\"message\":" + MAPPER.writeValueAsString(errorMsg) + "}");
            return;

          default:
            // ping, message_start, etc. — skip
            break;
        }
      } catch (Exception e) {
        logger.warn("Error parsing Anthropic SSE event: {}", data, e);
      }
    }
  }

  private void handleContentBlockStart(JsonNode event,
      Map<Integer, String> blockTypes,
      Map<Integer, String> toolUseIds,
      Map<Integer, String> toolUseNames,
      OutputStream out) throws Exception {

    int index = event.path("index").asInt(0);
    JsonNode block = event.get("content_block");
    if (block == null) {
      return;
    }

    String blockType = block.path("type").asText();
    blockTypes.put(index, blockType);

    if ("tool_use".equals(blockType)) {
      String id = block.path("id").asText();
      String name = block.path("name").asText();
      toolUseIds.put(index, id);
      toolUseNames.put(index, name);

      writeSseEvent(out, "delta",
          "{\"type\":\"tool_call_start\",\"id\":" + MAPPER.writeValueAsString(id) +
          ",\"name\":" + MAPPER.writeValueAsString(name) + "}");
    }
  }

  private void handleContentBlockDelta(JsonNode event,
      Map<Integer, String> blockTypes,
      Map<Integer, String> toolUseIds,
      OutputStream out) throws Exception {

    int index = event.path("index").asInt(0);
    JsonNode delta = event.get("delta");
    if (delta == null) {
      return;
    }

    String deltaType = delta.path("type").asText();
    String blockType = blockTypes.getOrDefault(index, "text");

    if ("text_delta".equals(deltaType)) {
      String text = delta.path("text").asText();
      if (!text.isEmpty()) {
        writeSseEvent(out, "delta",
            "{\"type\":\"content\",\"content\":" + MAPPER.writeValueAsString(text) + "}");
      }
    } else if ("input_json_delta".equals(deltaType)) {
      String partial = delta.path("partial_json").asText();
      String id = toolUseIds.getOrDefault(index, "");
      if (!partial.isEmpty()) {
        writeSseEvent(out, "delta",
            "{\"type\":\"tool_call_delta\",\"id\":" + MAPPER.writeValueAsString(id) +
            ",\"arguments\":" + MAPPER.writeValueAsString(partial) + "}");
      }
    }
  }

  private void handleContentBlockStop(JsonNode event,
      Map<Integer, String> blockTypes,
      Map<Integer, String> toolUseIds,
      OutputStream out) throws Exception {

    int index = event.path("index").asInt(0);
    String blockType = blockTypes.getOrDefault(index, "text");

    if ("tool_use".equals(blockType)) {
      String id = toolUseIds.getOrDefault(index, "");
      writeSseEvent(out, "delta",
          "{\"type\":\"tool_call_end\",\"id\":" + MAPPER.writeValueAsString(id) + "}");
    }
  }

  private static void writeSseEvent(OutputStream out, String event, String data) throws Exception {
    String sse = "event: " + event + "\ndata: " + data + "\n\n";
    out.write(sse.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }
}
