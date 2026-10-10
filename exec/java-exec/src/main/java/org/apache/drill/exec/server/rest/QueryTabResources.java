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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * CRUD for SQL Lab query tabs.
 *
 * <p>Tabs are per-user. The owner is always taken from the authenticated principal and
 * never from the request body, so no caller can write into another user's namespace.
 *
 * <p>See {@code docs/dev/TabPersistence.md} for when a tab reaches this store at all:
 * unpromoted tabs live only in the browser.
 */
@Path("/api/v1/tabs")
@Tag(name = "Query Tabs", description = "APIs for per-user SQL Lab query tabs")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryTabResources {

  private static final Logger logger = LoggerFactory.getLogger(QueryTabResources.class);

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  public static class TabsResponse {
    @JsonProperty
    public List<QueryTabStore.TabRecord> tabs;

    public TabsResponse(List<QueryTabStore.TabRecord> tabs) {
      this.tabs = tabs;
    }
  }

  public static class MessageResponse {
    @JsonProperty
    public String message;

    public MessageResponse(String message) {
      this.message = message;
    }
  }

  private QueryTabStore getStore() {
    return QueryTabStore.get(storeProvider, workManager);
  }

  private String getCurrentUser() {
    return principal.getName();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List query tabs",
      description = "Returns the calling user's tabs for a project, or their global tabs "
          + "when projectId is omitted. Hidden tabs are included: hiding is not deletion.")
  public TabsResponse listTabs(
      @Parameter(description = "Project ID; omit for global tabs")
      @QueryParam("projectId") String projectId) {
    logger.debug("Listing tabs for project {}", projectId);
    return new TabsResponse(getStore().list(getCurrentUser(), projectId));
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create a query tab",
      description = "Stores a tab for the calling user. The server sets the owner and "
          + "timestamps, and generates an id when the body omits one.")
  public Response createTab(QueryTabStore.TabRecord tab) {
    try {
      if (tab.getId() == null || tab.getId().isEmpty()) {
        tab.setId(UUID.randomUUID().toString());
      }
      long now = Instant.now().toEpochMilli();
      tab.setOwner(getCurrentUser());
      tab.setCreatedAt(now);
      tab.setUpdatedAt(now);

      synchronized (tab.getId().intern()) {
        getStore().save(tab);
      }
      return Response.ok(tab).build();
    } catch (Exception e) {
      logger.error("Error creating tab", e);
      throw new DrillRuntimeException("Failed to create tab: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update a query tab",
      description = "Replaces a tab's contents. Only the owner may update it.")
  public Response updateTab(
      @Parameter(description = "Tab ID") @PathParam("id") String id,
      QueryTabStore.TabRecord tab) {
    try {
      synchronized (id.intern()) {
        QueryTabStore store = getStore();
        QueryTabStore.TabRecord existing = store.find(id);

        if (existing == null) {
          return Response.status(Response.Status.NOT_FOUND)
              .entity(new MessageResponse("Tab not found"))
              .build();
        }
        if (!getCurrentUser().equals(existing.getOwner())) {
          return Response.status(Response.Status.FORBIDDEN)
              .entity(new MessageResponse("Only the owner can modify this tab"))
              .build();
        }

        tab.setId(id);
        tab.setOwner(existing.getOwner());
        tab.setCreatedAt(existing.getCreatedAt());
        tab.setUpdatedAt(Instant.now().toEpochMilli());

        // The project a tab belongs to is part of its store key, so letting an update
        // change it would strand the old record under the previous key.
        tab.setProjectId(existing.getProjectId());

        store.save(tab);
        return Response.ok(tab).build();
      }
    } catch (Exception e) {
      logger.error("Error updating tab", e);
      throw new DrillRuntimeException("Failed to update tab: " + e.getMessage(), e);
    }
  }

  @GET
  @Path("/{id}/conversation")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get a tab's Prospector conversation",
      description = "Returns the chat history for one tab. A tab that has not been "
          + "talked to returns an empty message list rather than a 404.")
  public Response getConversation(
      @Parameter(description = "Tab ID") @PathParam("id") String id) {
    QueryTabStore.TabRecord tab = getStore().find(id);
    if (tab == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(new MessageResponse("Tab not found"))
          .build();
    }
    if (!getCurrentUser().equals(tab.getOwner())) {
      return Response.status(Response.Status.FORBIDDEN)
          .entity(new MessageResponse("Only the owner can read this conversation"))
          .build();
    }
    return Response.ok(TabConversationStore.get(storeProvider, workManager).find(id)).build();
  }

  @PUT
  @Path("/{id}/conversation")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Replace a tab's Prospector conversation",
      description = "Stores the chat history for one tab. Rejects payloads over 512 KB "
          + "with 413: the default store writes into a ZooKeeper znode, whose 1 MB "
          + "limit would otherwise fail the write far from where it can be reported.")
  public Response putConversation(
      @Parameter(description = "Tab ID") @PathParam("id") String id,
      TabConversationStore.Conversation conversation) {
    try {
      QueryTabStore.TabRecord tab = getStore().find(id);
      if (tab == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Tab not found"))
            .build();
      }
      if (!getCurrentUser().equals(tab.getOwner())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can write this conversation"))
            .build();
      }

      // Measured on the serialized form, which is what actually reaches the store.
      // Rejected rather than truncated: a conversation silently losing its earliest
      // messages is worse than a refused write the caller can surface.
      byte[] serialized = workManager.getContext().getLpPersistence().getMapper()
          .writeValueAsBytes(conversation);
      if (serialized.length > TabConversationStore.MAX_CONVERSATION_BYTES) {
        return Response.status(413)
            .entity(new MessageResponse(String.format(
                "Conversation is %d bytes, over the %d byte limit. Start a new tab to "
                    + "continue.", serialized.length,
                TabConversationStore.MAX_CONVERSATION_BYTES)))
            .build();
      }

      synchronized (id.intern()) {
        TabConversationStore.get(storeProvider, workManager).save(id, conversation);
      }
      return Response.ok(conversation).build();
    } catch (Exception e) {
      logger.error("Error saving tab conversation", e);
      throw new DrillRuntimeException("Failed to save conversation: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete a query tab",
      description = "Permanently removes a tab. Locked tabs cannot be deleted; hide them "
          + "instead.")
  public Response deleteTab(@Parameter(description = "Tab ID") @PathParam("id") String id) {
    try {
      synchronized (id.intern()) {
        QueryTabStore store = getStore();
        QueryTabStore.TabRecord existing = store.find(id);

        if (existing == null) {
          return Response.status(Response.Status.NOT_FOUND)
              .entity(new MessageResponse("Tab not found"))
              .build();
        }
        if (!getCurrentUser().equals(existing.getOwner())) {
          return Response.status(Response.Status.FORBIDDEN)
              .entity(new MessageResponse("Only the owner can delete this tab"))
              .build();
        }
        if (existing.isLocked()) {
          return Response.status(Response.Status.CONFLICT)
              .entity(new MessageResponse(
                  "This tab is locked and cannot be deleted. Unlock it first, or hide it."))
              .build();
        }

        store.delete(id);
        // The conversation belongs to the tab; leaving it would orphan a record
        // nothing can reach.
        TabConversationStore.get(storeProvider, workManager).delete(id);
        return Response.ok(new MessageResponse("Tab deleted")).build();
      }
    } catch (Exception e) {
      logger.error("Error deleting tab", e);
      throw new DrillRuntimeException("Failed to delete tab: " + e.getMessage(), e);
    }
  }
}
