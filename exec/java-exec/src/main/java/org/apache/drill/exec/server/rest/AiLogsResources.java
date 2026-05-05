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
import org.apache.drill.exec.server.rest.ai.AiEvent;
import org.apache.drill.exec.server.rest.ai.AiEventLogger;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import java.security.Principal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * REST endpoint that accepts client-side AI observability events and appends
 * them to the JSONL ai-events.log used by the AI analytics dashboard.
 * The user identity is taken from the SecurityContext on the server, not the request body.
 */
@Path("/api/v1/ai/logs")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class AiLogsResources {
  private static final Logger logger = LoggerFactory.getLogger(AiLogsResources.class);

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response logAiInteractions(List<AiLogEntry> entries, @Context SecurityContext sc) {
    if (entries == null || entries.isEmpty()) {
      return Response.ok(new AiLogResponse(true, "No entries to log")).build();
    }

    String username = resolveUser(sc);
    int written = 0;
    for (AiLogEntry entry : entries) {
      try {
        AiEventLogger.log(toEvent(entry, username));
        written++;
      } catch (Exception e) {
        logger.warn("Failed to log AI event entry", e);
      }
    }
    return Response.ok(new AiLogResponse(true, "Logged " + written + " AI interactions")).build();
  }

  private static String resolveUser(SecurityContext sc) {
    if (sc == null) {
      return "anonymous";
    }
    Principal p = sc.getUserPrincipal();
    if (p == null || p.getName() == null || p.getName().isEmpty()) {
      return "anonymous";
    }
    return p.getName();
  }

  private static AiEvent toEvent(AiLogEntry entry, String username) {
    AiEvent event = new AiEvent();
    if (entry.timestamp > 0) {
      event.ts = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(entry.timestamp));
    } else {
      event.ts = AiEventLogger.nowIso();
    }
    event.user = username;
    event.feature = entry.type;
    event.source = "client";
    event.provider = entry.provider;
    event.model = entry.model;
    event.promptTokens = entry.promptTokens > 0 ? entry.promptTokens : null;
    event.responseTokens = entry.responseTokens > 0 ? entry.responseTokens : null;
    event.totalTokens = entry.totalTokens > 0 ? entry.totalTokens : null;
    event.durationMs = entry.duration > 0 ? entry.duration : null;
    event.success = entry.success;
    event.error = entry.error;
    if (entry.error != null && !entry.error.isEmpty()) {
      event.errorClass = "ClientReported";
    }
    event.userMessage = AiEventLogger.truncate(entry.prompt);
    event.prompt = AiEventLogger.truncate(entry.prompt);
    event.response = AiEventLogger.truncate(entry.response);
    return event;
  }

  /** Request body for logging AI interactions from the browser. */
  public static class AiLogEntry {
    @JsonProperty public long timestamp;
    @JsonProperty public String type;
    @JsonProperty public String provider;
    @JsonProperty public String model;
    @JsonProperty public int promptTokens;
    @JsonProperty public int responseTokens;
    @JsonProperty public int totalTokens;
    @JsonProperty public long duration;
    @JsonProperty public boolean success;
    @JsonProperty public String error;
    @JsonProperty public String prompt;
    @JsonProperty public String response;
  }

  public static class AiLogResponse {
    @JsonProperty public final boolean success;
    @JsonProperty public final String message;

    public AiLogResponse(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }
}
