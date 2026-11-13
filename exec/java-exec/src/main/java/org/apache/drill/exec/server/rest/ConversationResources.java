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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.server.rest.ConversationStore.Conversation;
import org.apache.drill.exec.server.rest.ConversationStore.ConversationRecord;
import org.apache.drill.exec.server.rest.ai.ChatMessage;
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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * CRUD for project-scoped Prospector conversations.
 *
 * <p>Conversations are per-user. The owner is always taken from the authenticated
 * principal and never from the request body.
 */
@Path("/api/v1/projects/{projectId}/conversations")
@Tag(name = "Conversations", description = "APIs for project-scoped Prospector conversations")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ConversationResources {

  private static final Logger logger = LoggerFactory.getLogger(ConversationResources.class);

  private static final String DEFAULT_NAME = "New conversation";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  public static class ConversationsResponse {
    @JsonProperty
    public List<ConversationRecord> conversations;

    public ConversationsResponse(List<ConversationRecord> conversations) {
      this.conversations = conversations;
    }
  }

  public static class MessageResponse {
    @JsonProperty
    public String message;

    public MessageResponse(String message) {
      this.message = message;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CreateRequest {
    @JsonProperty
    public String name;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RenameRequest {
    @JsonProperty
    public String name;
  }

  public static class ConversationDetail {
    @JsonProperty
    public ConversationRecord record;

    @JsonProperty
    public List<ChatMessage> messages;

    public ConversationDetail(ConversationRecord record, List<ChatMessage> messages) {
      this.record = record;
      this.messages = messages;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class MessagesRequest {
    @JsonProperty
    public List<ChatMessage> messages;

    @JsonProperty
    public int version;
  }

  public static class VersionResponse {
    @JsonProperty
    public int version;

    public VersionResponse(int version) {
      this.version = version;
    }
  }

  private ConversationStore getStore() {
    return ConversationStore.get(storeProvider, workManager);
  }

  private String getCurrentUser() {
    return principal.getName();
  }

  private static Response error(Response.Status status, String message) {
    return Response.status(status).entity(new MessageResponse(message)).build();
  }

  /**
   * Gates every endpoint on project access, using {@link ProjectResources#canRead} so the
   * rule is the one definition rather than a copy. A missing, trashed or unreadable project
   * is the same 404: a caller who may not read a project must not be able to tell it exists.
   * Returns null when the caller may proceed.
   */
  private Response checkProject(String projectId) {
    ProjectResources.Project project =
        ProjectResources.openStore(storeProvider, workManager).get(projectId);
    if (project == null || project.getDeletedAt() > 0
        || !ProjectResources.canRead(project, getCurrentUser())) {
      return error(Response.Status.NOT_FOUND, "Project not found");
    }
    return null;
  }

  /**
   * Returns an error response when the project is not accessible, the conversation is
   * missing, belongs to another project, or is owned by someone else; null when the caller
   * may proceed.
   */
  private Response check(ConversationRecord record, String projectId) {
    Response noProject = checkProject(projectId);
    if (noProject != null) {
      return noProject;
    }
    if (record == null || !Objects.equals(projectId, record.getProjectId())) {
      return error(Response.Status.NOT_FOUND, "Conversation not found");
    }
    if (!getCurrentUser().equals(record.getOwner())) {
      return error(Response.Status.FORBIDDEN, "Only the owner can access this conversation");
    }
    return null;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List conversations",
      description = "Returns the calling user's conversations in a project.")
  public Response list(
      @Parameter(description = "Project ID") @PathParam("projectId") String projectId) {
    Response noProject = checkProject(projectId);
    if (noProject != null) {
      return noProject;
    }
    return Response.ok(
        new ConversationsResponse(getStore().listForProject(projectId, getCurrentUser()))).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create a conversation",
      description = "The server sets the id, owner and timestamps.")
  public Response create(
      @Parameter(description = "Project ID") @PathParam("projectId") String projectId,
      CreateRequest request) {
    Response noProject = checkProject(projectId);
    if (noProject != null) {
      return noProject;
    }
    ConversationRecord record = new ConversationRecord();
    record.setId(UUID.randomUUID().toString());
    record.setProjectId(projectId);
    record.setOwner(getCurrentUser());
    String name = request != null ? request.name : null;
    record.setName(name == null || name.trim().isEmpty() ? DEFAULT_NAME : name.trim());
    long now = Instant.now().toEpochMilli();
    record.setCreatedAt(now);
    record.setUpdatedAt(now);
    getStore().saveRecord(record);
    return Response.status(Response.Status.CREATED).entity(record).build();
  }

  @GET
  @Path("/{cid}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get a conversation", description = "Returns the record and its messages.")
  public Response get(
      @Parameter(description = "Project ID") @PathParam("projectId") String projectId,
      @Parameter(description = "Conversation ID") @PathParam("cid") String cid) {
    ConversationStore store = getStore();
    ConversationRecord record = store.findRecord(cid);
    Response denied = check(record, projectId);
    if (denied != null) {
      return denied;
    }
    return Response.ok(new ConversationDetail(record, store.findMessages(cid).getMessages()))
        .build();
  }

  @PUT
  @Path("/{cid}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Rename a conversation")
  public Response rename(
      @Parameter(description = "Project ID") @PathParam("projectId") String projectId,
      @Parameter(description = "Conversation ID") @PathParam("cid") String cid,
      RenameRequest request) {
    if (request == null || request.name == null || request.name.trim().isEmpty()) {
      return error(Response.Status.BAD_REQUEST, "name is required");
    }
    synchronized (cid.intern()) {
      ConversationStore store = getStore();
      ConversationRecord record = store.findRecord(cid);
      Response denied = check(record, projectId);
      if (denied != null) {
        return denied;
      }
      record.setName(request.name.trim());
      record.setUpdatedAt(Instant.now().toEpochMilli());
      store.updateMetadata(record);
      return Response.ok(record).build();
    }
  }

  @PUT
  @Path("/{cid}/messages")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Replace a conversation's messages",
      description = "Rejects a stale version with 409 and payloads over 512 KB with 413: "
          + "the default store writes into a ZooKeeper znode whose 1 MB limit would "
          + "otherwise fail the write far from where it can be reported.")
  public Response putMessages(
      @Parameter(description = "Project ID") @PathParam("projectId") String projectId,
      @Parameter(description = "Conversation ID") @PathParam("cid") String cid,
      MessagesRequest request) {
    try {
      ConversationStore store = getStore();
      ConversationRecord existing = store.findRecord(cid);
      Response denied = check(existing, projectId);
      if (denied != null) {
        return denied;
      }
      if (request == null || request.messages == null) {
        return error(Response.Status.BAD_REQUEST, "messages is required");
      }

      Conversation conversation = new Conversation(request.messages);
      // Measured on the serialized form, which is what actually reaches the store.
      byte[] serialized = workManager.getContext().getLpPersistence().getMapper()
          .writeValueAsBytes(conversation);
      if (serialized.length > ConversationStore.MAX_CONVERSATION_BYTES) {
        return error(Response.Status.fromStatusCode(413), String.format(
            "Conversation is %d bytes, over the %d byte limit. Start a new conversation "
                + "to continue.", serialized.length, ConversationStore.MAX_CONVERSATION_BYTES));
      }

      synchronized (cid.intern()) {
        // Re-read under the lock so two writers cannot both pass the version check.
        ConversationRecord record = store.findRecord(cid);
        if (record == null) {
          return error(Response.Status.NOT_FOUND, "Conversation not found");
        }
        if (record.getVersion() != request.version) {
          return error(Response.Status.CONFLICT,
              "Conversation was changed elsewhere. Reload it before saving.");
        }
        store.saveMessages(cid, conversation);
        record.setMessageCount(request.messages.size());
        record.setUpdatedAt(Instant.now().toEpochMilli());
        store.saveRecord(record);
        return Response.ok(new VersionResponse(record.getVersion())).build();
      }
    } catch (Exception e) {
      logger.error("Error saving conversation messages", e);
      throw new DrillRuntimeException("Failed to save conversation: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{cid}")
  @Operation(summary = "Delete a conversation")
  public Response delete(
      @Parameter(description = "Project ID") @PathParam("projectId") String projectId,
      @Parameter(description = "Conversation ID") @PathParam("cid") String cid) {
    synchronized (cid.intern()) {
      ConversationStore store = getStore();
      Response denied = check(store.findRecord(cid), projectId);
      if (denied != null) {
        return denied;
      }
      store.delete(cid);
      return Response.noContent().build();
    }
  }
}
