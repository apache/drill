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
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.ai.ChatMessage;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Project-scoped Prospector conversations. Metadata records live in the
 * {@code drill.sqllab.conversations} PersistentStore and message bodies in
 * {@code drill.sqllab.conversation_messages}.
 *
 * <p>The two are kept apart on purpose. The project tree lists conversations on every
 * expand; carrying chat history in that listing would make an already chatty request
 * enormous.
 *
 * <p>Message bodies are capped at {@link #MAX_CONVERSATION_BYTES}. {@code PersistentStore}
 * defaults to {@code ZookeeperPersistentStoreProvider}, which writes serialized bytes
 * straight into a znode, and ZooKeeper's default {@code jute.maxbuffer} is 1 MB. Without
 * a cap the failure would surface deep in the ZooKeeper layer, long after the user could
 * do anything about it.
 */
public class ConversationStore {

  private static final String RECORD_STORE_NAME = "drill.sqllab.conversations";
  private static final String MESSAGE_STORE_NAME = "drill.sqllab.conversation_messages";

  /**
   * Half of ZooKeeper's 1 MB znode default, leaving room for the JSON envelope and
   * for a deployment that has tightened jute.maxbuffer rather than raised it.
   */
  public static final int MAX_CONVERSATION_BYTES = 512 * 1024;

  private static volatile ConversationStore instance;

  private final PersistentStore<ConversationRecord> recordStore;
  private final PersistentStore<Conversation> messageStore;

  ConversationStore(PersistentStore<ConversationRecord> recordStore,
                    PersistentStore<Conversation> messageStore) {
    this.recordStore = recordStore;
    this.messageStore = messageStore;
  }

  public static ConversationStore get(PersistentStoreProvider provider, WorkManager workManager) {
    if (instance == null) {
      synchronized (ConversationStore.class) {
        if (instance == null) {
          try {
            PersistentStore<ConversationRecord> records = provider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    ConversationRecord.class)
                    .name(RECORD_STORE_NAME)
                    .build());
            PersistentStore<Conversation> messages = provider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    Conversation.class)
                    .name(MESSAGE_STORE_NAME)
                    .build());
            instance = new ConversationStore(records, messages);
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access conversation store", e);
          }
        }
      }
    }
    return instance;
  }

  /**
   * Every conversation one user owns in one project. {@link PersistentStore} has no
   * prefix scan, so this filters {@code getAll()}; conversation counts per user are small.
   */
  public List<ConversationRecord> listForProject(String projectId, String owner) {
    List<ConversationRecord> found = new ArrayList<>();
    Iterator<Map.Entry<String, ConversationRecord>> entries = recordStore.getAll();
    while (entries.hasNext()) {
      ConversationRecord record = entries.next().getValue();
      if (record != null
          && Objects.equals(projectId, record.getProjectId())
          && Objects.equals(owner, record.getOwner())) {
        found.add(record);
      }
    }
    return found;
  }

  /**
   * Looks a record up by id. Callers must still check ownership before acting on the
   * result, since this returns any user's conversation.
   */
  public ConversationRecord findRecord(String id) {
    return recordStore.get(id);
  }

  /** Bumps the record's version, then stores it. */
  public void saveRecord(ConversationRecord record) {
    record.setVersion(record.getVersion() + 1);
    recordStore.put(record.getId(), record);
  }

  /** Never null: a conversation that has not been talked to simply has no messages yet. */
  public Conversation findMessages(String conversationId) {
    Conversation stored = messageStore.get(conversationId);
    return stored != null ? stored : new Conversation(new ArrayList<>());
  }

  public void saveMessages(String conversationId, Conversation conversation) {
    messageStore.put(conversationId, conversation);
  }

  /** Removes the record and its messages. */
  public void delete(String conversationId) {
    recordStore.delete(conversationId);
    messageStore.delete(conversationId);
  }

  /** One conversation's metadata. Messages are stored separately. */
  public static class ConversationRecord {

    @JsonProperty
    private String id;

    @JsonProperty
    private String projectId;

    /** Always set server-side from the authenticated principal, never from the body. */
    @JsonProperty
    private String owner;

    @JsonProperty
    private String name;

    @JsonProperty
    private long createdAt;

    @JsonProperty
    private long updatedAt;

    @JsonProperty
    private int messageCount;

    @JsonProperty
    private int version;

    public ConversationRecord() {
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getProjectId() {
      return projectId;
    }

    public void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    public String getOwner() {
      return owner;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public void setCreatedAt(long createdAt) {
      this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
      return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
      this.updatedAt = updatedAt;
    }

    public int getMessageCount() {
      return messageCount;
    }

    public void setMessageCount(int messageCount) {
      this.messageCount = messageCount;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }
  }

  /** One conversation's chat history. */
  public static class Conversation {

    @JsonProperty
    private List<ChatMessage> messages;

    public Conversation() {
      this.messages = new ArrayList<>();
    }

    public Conversation(List<ChatMessage> messages) {
      this.messages = messages != null ? messages : new ArrayList<>();
    }

    public List<ChatMessage> getMessages() {
      return messages;
    }

    public void setMessages(List<ChatMessage> messages) {
      this.messages = messages != null ? messages : new ArrayList<>();
    }
  }
}
