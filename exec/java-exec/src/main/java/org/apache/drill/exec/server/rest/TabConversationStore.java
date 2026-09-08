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
import java.util.List;

/**
 * Prospector conversations, one per query tab, in the
 * {@code drill.sqllab.tab_conversations} PersistentStore.
 *
 * <p>Kept apart from {@link QueryTabStore} on purpose. A conversation is far larger
 * than a tab record, and the project tree lists tabs on every expand; carrying chat
 * history in that listing would make an already chatty request enormous.
 *
 * <p>Conversations are capped at {@link #MAX_CONVERSATION_BYTES}. {@code PersistentStore}
 * defaults to {@code ZookeeperPersistentStoreProvider}, which writes serialized bytes
 * straight into a znode, and ZooKeeper's default {@code jute.maxbuffer} is 1 MB. Without
 * a cap here the failure would surface deep in the ZooKeeper layer, long after the user
 * could do anything about it.
 */
public class TabConversationStore {

  private static final String STORE_NAME = "drill.sqllab.tab_conversations";

  /**
   * Half of ZooKeeper's 1 MB znode default, leaving room for the JSON envelope and
   * for a deployment that has tightened jute.maxbuffer rather than raised it.
   */
  public static final int MAX_CONVERSATION_BYTES = 512 * 1024;

  private static volatile PersistentStore<Conversation> cachedStore;
  private static volatile TabConversationStore instance;

  private final PersistentStore<Conversation> store;

  public TabConversationStore(PersistentStore<Conversation> store) {
    this.store = store;
  }

  public static TabConversationStore get(PersistentStoreProvider provider, WorkManager workManager) {
    if (instance == null) {
      synchronized (TabConversationStore.class) {
        if (instance == null) {
          try {
            cachedStore = provider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    Conversation.class)
                    .name(STORE_NAME)
                    .build());
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access tab conversation store", e);
          }
          instance = new TabConversationStore(cachedStore);
        }
      }
    }
    return instance;
  }

  /** Never null: a tab that has not been talked to simply has no messages yet. */
  public Conversation find(String tabId) {
    Conversation stored = store.get(tabId);
    return stored != null ? stored : new Conversation(new ArrayList<>());
  }

  public void save(String tabId, Conversation conversation) {
    store.put(tabId, conversation);
  }

  public void delete(String tabId) {
    store.delete(tabId);
  }

  /** One tab's chat history. */
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
