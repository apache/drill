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
 * LLM provider for OpenAI-compatible APIs (OpenAI, Azure OpenAI, Ollama, etc.).
 * Uses OkHttp to call /chat/completions with stream:true,
 * reads SSE line-by-line, and normalizes deltas to the common wire format.
 */
public class OpenAiCompatibleProvider implements LlmProvider {

  private static final Logger logger = LoggerFactory.getLogger(OpenAiCompatibleProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MediaType JSON_TYPE = MediaType.parse("application/json");
  private static final String DEFAULT_ENDPOINT = "https://api.openai.com/v1";

  private final OkHttpClient httpClient;

  public OpenAiCompatibleProvider() {
    this.httpClient = new OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .build();
  }

  @Override
  public String getId() {
    return "openai";
  }

  @Override
  public String getDisplayName() {
    return "OpenAI Compatible";
  }

  @Override
  public void streamChatCompletion(LlmConfig config, List<ChatMessage> messages,
      List<ToolDefinition> tools, OutputStream out) throws Exception {

    String endpoint = config.getApiEndpoint();
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = DEFAULT_ENDPOINT;
    }
    // Remove trailing slash
    if (endpoint.endsWith("/")) {
      endpoint = endpoint.substring(0, endpoint.length() - 1);
    }
    String url = endpoint + "/chat/completions";

    ObjectNode requestBody = buildRequestBody(config, messages, tools);

    Request.Builder reqBuilder = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestBody.toString(), JSON_TYPE))
        .addHeader("Content-Type", "application/json");

    if (config.getApiKey() != null && !config.getApiKey().isEmpty()) {
      reqBuilder.addHeader("Authorization", "Bearer " + config.getApiKey());
    }

    Request request = reqBuilder.build();

    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        String errorBody = "";
        ResponseBody body = response.body();
        if (body != null) {
          errorBody = body.string();
        }
        String errorMsg = "LLM API error " + response.code() + ": " + errorBody;
        logger.error(errorMsg);
        writeSseEvent(out, "error", "{\"message\":" + MAPPER.writeValueAsString(errorMsg) + "}");
        return;
      }

      ResponseBody body = response.body();
      if (body == null) {
        writeSseEvent(out, "error", "{\"message\":\"Empty response from LLM API\"}");
        return;
      }

      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(body.byteStream(), StandardCharsets.UTF_8))) {
        processOpenAiStream(reader, out);
      }
    }
  }

  @Override
  public ValidationResult validateConfig(LlmConfig config) {
    if (config.getApiKey() == null || config.getApiKey().isEmpty()) {
      // Ollama doesn't require an API key, so only warn
      String endpoint = config.getApiEndpoint();
      if (endpoint != null && !endpoint.contains("localhost") && !endpoint.contains("127.0.0.1")) {
        return ValidationResult.error("API key is required for non-local endpoints");
      }
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
    body.put("stream", true);
    body.put("max_tokens", config.getMaxTokens());
    body.put("temperature", config.getTemperature());

    // Messages
    ArrayNode messagesArray = body.putArray("messages");
    for (ChatMessage msg : messages) {
      ObjectNode msgNode = messagesArray.addObject();
      msgNode.put("role", msg.getRole());

      if (msg.getContent() != null) {
        msgNode.put("content", msg.getContent());
      }

      if (msg.getToolCallId() != null) {
        msgNode.put("tool_call_id", msg.getToolCallId());
      }

      if (msg.getName() != null) {
        msgNode.put("name", msg.getName());
      }

      if (msg.getToolCalls() != null && !msg.getToolCalls().isEmpty()) {
        ArrayNode toolCallsNode = msgNode.putArray("tool_calls");
        for (ToolCall tc : msg.getToolCalls()) {
          ObjectNode tcNode = toolCallsNode.addObject();
          tcNode.put("id", tc.getId());
          tcNode.put("type", "function");
          ObjectNode fnNode = tcNode.putObject("function");
          fnNode.put("name", tc.getName());
          fnNode.put("arguments", tc.getArguments());
        }
      }
    }

    // Tools
    if (tools != null && !tools.isEmpty()) {
      ArrayNode toolsArray = body.putArray("tools");
      for (ToolDefinition tool : tools) {
        ObjectNode toolNode = toolsArray.addObject();
        toolNode.put("type", "function");
        ObjectNode fnNode = toolNode.putObject("function");
        fnNode.put("name", tool.getName());
        fnNode.put("description", tool.getDescription());
        fnNode.set("parameters", MAPPER.valueToTree(tool.getParameters()));
      }
    }

    return body;
  }

  private void processOpenAiStream(BufferedReader reader, OutputStream out) throws Exception {
    // Track tool calls being assembled
    Map<Integer, String> toolCallIds = new HashMap<>();
    Map<Integer, String> toolCallNames = new HashMap<>();
    Map<Integer, StringBuilder> toolCallArgs = new HashMap<>();

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

      if ("[DONE]".equals(data)) {
        // Check if we have pending tool calls
        if (!toolCallIds.isEmpty()) {
          // Emit tool_call_end events
          for (Map.Entry<Integer, String> entry : toolCallIds.entrySet()) {
            int idx = entry.getKey();
            writeSseEvent(out, "delta",
                "{\"type\":\"tool_call_end\",\"id\":" + MAPPER.writeValueAsString(entry.getValue()) + "}");
          }
          writeSseEvent(out, "done", "{\"finish_reason\":\"tool_calls\"}");
        } else {
          writeSseEvent(out, "done", "{\"finish_reason\":\"stop\"}");
        }
        return;
      }

      try {
        JsonNode chunk = MAPPER.readTree(data);
        JsonNode choices = chunk.get("choices");
        if (choices == null || !choices.isArray() || choices.isEmpty()) {
          continue;
        }

        JsonNode choice = choices.get(0);
        JsonNode delta = choice.get("delta");
        if (delta == null) {
          continue;
        }

        // Check finish_reason
        JsonNode finishReason = choice.get("finish_reason");
        if (finishReason != null && !finishReason.isNull()) {
          String reason = finishReason.asText();
          if ("tool_calls".equals(reason)) {
            // Emit end events for all tracked tool calls
            for (Map.Entry<Integer, String> entry : toolCallIds.entrySet()) {
              writeSseEvent(out, "delta",
                  "{\"type\":\"tool_call_end\",\"id\":" + MAPPER.writeValueAsString(entry.getValue()) + "}");
            }
            writeSseEvent(out, "done", "{\"finish_reason\":\"tool_calls\"}");
            return;
          }
          if ("stop".equals(reason)) {
            writeSseEvent(out, "done", "{\"finish_reason\":\"stop\"}");
            return;
          }
        }

        // Content delta
        JsonNode content = delta.get("content");
        if (content != null && !content.isNull()) {
          String text = content.asText();
          if (!text.isEmpty()) {
            writeSseEvent(out, "delta",
                "{\"type\":\"content\",\"content\":" + MAPPER.writeValueAsString(text) + "}");
          }
        }

        // Tool call deltas
        JsonNode toolCallsNode = delta.get("tool_calls");
        if (toolCallsNode != null && toolCallsNode.isArray()) {
          for (JsonNode tc : toolCallsNode) {
            int idx = tc.has("index") ? tc.get("index").asInt() : 0;

            // New tool call start
            if (tc.has("id")) {
              String id = tc.get("id").asText();
              toolCallIds.put(idx, id);
              String name = "";
              if (tc.has("function") && tc.get("function").has("name")) {
                name = tc.get("function").get("name").asText();
              }
              toolCallNames.put(idx, name);
              toolCallArgs.put(idx, new StringBuilder());

              writeSseEvent(out, "delta",
                  "{\"type\":\"tool_call_start\",\"id\":" + MAPPER.writeValueAsString(id) +
                  ",\"name\":" + MAPPER.writeValueAsString(name) + "}");
            }

            // Argument delta
            if (tc.has("function") && tc.get("function").has("arguments")) {
              String argDelta = tc.get("function").get("arguments").asText();
              if (argDelta != null && !argDelta.isEmpty()) {
                StringBuilder args = toolCallArgs.get(idx);
                if (args != null) {
                  args.append(argDelta);
                }
                String tcId = toolCallIds.getOrDefault(idx, "");
                writeSseEvent(out, "delta",
                    "{\"type\":\"tool_call_delta\",\"id\":" + MAPPER.writeValueAsString(tcId) +
                    ",\"arguments\":" + MAPPER.writeValueAsString(argDelta) + "}");
              }
            }
          }
        }
      } catch (Exception e) {
        logger.warn("Error parsing SSE chunk: {}", data, e);
      }
    }
  }

  private static void writeSseEvent(OutputStream out, String event, String data) throws Exception {
    String sse = "event: " + event + "\ndata: " + data + "\n\n";
    out.write(sse.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }
}
