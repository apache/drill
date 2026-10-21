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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Appends AiEvent records as JSON lines to ai-events.log via the "ai.events"
 * SLF4J logger. The dedicated logback appender (configured in
 * distribution/src/main/resources/logback.xml) handles file naming, rotation,
 * and disk-cap. Drill queries the resulting files via the dfs.ai_logs workspace.
 */
public final class AiEventLogger {

  /** Diagnostic logger — only used to surface serialization failures. */
  private static final Logger diag = LoggerFactory.getLogger(AiEventLogger.class);

  /**
   * Dedicated event-stream logger. Wired in logback.xml to the AI_EVENTS rolling
   * appender with a "%msg%n" pattern, so each .info() call writes exactly one
   * JSON line — no level/thread/timestamp decoration in front.
   */
  private static final Logger events = LoggerFactory.getLogger("ai.events");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Cap stored prompt and response sizes to keep individual log lines bounded. */
  private static final int MAX_TEXT_LEN = 50_000;

  private AiEventLogger() {
  }

  /** ISO-8601 UTC timestamp suitable for the ts field. */
  public static String nowIso() {
    return DateTimeFormatter.ISO_INSTANT.format(Instant.now());
  }

  /**
   * True when the throwable indicates the client closed the connection mid-stream
   * (Jetty's EofException, or IOException with a "broken pipe"/"closed"/"reset"
   * message). Walks the cause chain.
   */
  public static boolean isClientCancellation(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      String name = cur.getClass().getSimpleName();
      if ("EofException".equals(name) || "ClosedChannelException".equals(name)) {
        return true;
      }
      String msg = cur.getMessage();
      if (msg != null) {
        String lower = msg.toLowerCase();
        if (lower.contains("broken pipe")
            || lower.contains("connection reset")
            || lower.contains("connection was aborted")
            || "closed".equals(lower.trim())) {
          return true;
        }
      }
      cur = cur.getCause();
    }
    return false;
  }

  /** Truncate a text field for storage. Returns null unchanged. */
  public static String truncate(String text) {
    if (text == null) {
      return null;
    }
    if (text.length() <= MAX_TEXT_LEN) {
      return text;
    }
    return text.substring(0, MAX_TEXT_LEN) + "...[truncated]";
  }

  /**
   * Append one event as a JSON line. Best-effort — serialization failures are
   * logged via the diagnostic logger and swallowed so analytics never break
   * the surrounding AI request. Logback handles file IO, rotation, and the
   * disk-cap from there.
   */
  public static void log(AiEvent event) {
    if (event == null) {
      return;
    }
    if (event.ts == null) {
      event.ts = nowIso();
    }
    try {
      events.info(MAPPER.writeValueAsString(event));
    } catch (Exception e) {
      diag.warn("Failed to serialize AI event", e);
    }
  }
}
