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

import org.apache.drill.exec.server.rest.ConversationStore.Conversation;
import org.apache.drill.exec.server.rest.ConversationStore.ConversationRecord;
import org.apache.drill.exec.server.rest.ai.ChatMessage;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreMode;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConversationStoreTest {

  private ConversationStore store;

  @Before
  public void setUp() {
    store = new ConversationStore(new MemoryStore<>(), new MemoryStore<>());
  }

  private static ConversationRecord record(String id, String projectId, String owner) {
    ConversationRecord r = new ConversationRecord();
    r.setId(id);
    r.setProjectId(projectId);
    r.setOwner(owner);
    r.setName("name-" + id);
    return r;
  }

  @Test
  public void listForProjectReturnsOnlyThisOwnersRecordsInThatProject() {
    store.saveRecord(record("a", "p1", "alice"));
    store.saveRecord(record("b", "p1", "bob"));
    store.saveRecord(record("c", "p2", "alice"));

    java.util.List<ConversationRecord> found = store.listForProject("p1", "alice");
    assertEquals(1, found.size());
    assertEquals("a", found.get(0).getId());
  }

  @Test
  public void findMessagesReturnsEmptyConversationWhenAbsent() {
    Conversation c = store.findMessages("missing");
    assertNotNull(c);
    assertNotNull(c.getMessages());
    assertTrue(c.getMessages().isEmpty());
  }

  @Test
  public void deleteRemovesBothRecordAndMessages() {
    store.saveRecord(record("a", "p1", "alice"));
    store.saveMessages("a", new Conversation(new ArrayList<>(Arrays.asList(ChatMessage.user("hi")))));
    assertEquals(1, store.findMessages("a").getMessages().size());

    store.delete("a");

    assertNull(store.findRecord("a"));
    assertTrue(store.findMessages("a").getMessages().isEmpty());
  }

  @Test
  public void failedMessageDeleteLeavesTheConversationReachable() {
    MemoryStore<ConversationRecord> records = new MemoryStore<>();
    MemoryStore<Conversation> messages = new MemoryStore<Conversation>() {
      @Override
      public void delete(String key) {
        throw new IllegalStateException("message store unavailable");
      }
    };
    ConversationStore failing = new ConversationStore(records, messages);
    failing.saveRecord(record("a", "p1", "alice"));
    failing.saveMessages("a", new Conversation(new ArrayList<>(Arrays.asList(ChatMessage.user("hi")))));

    try {
      failing.delete("a");
      fail("expected the message store failure to propagate");
    } catch (IllegalStateException expected) {
      // The failure is the point: what matters is what is left behind.
    }

    // The record survives, so the user can find the conversation and delete it again.
    assertNotNull(failing.findRecord("a"));
    assertEquals(1, failing.listForProject("p1", "alice").size());
  }

  @Test
  public void saveRecordIncrementsVersion() {
    ConversationRecord r = record("a", "p1", "alice");
    r.setVersion(3);
    store.saveRecord(r);
    assertEquals(4, store.findRecord("a").getVersion());
  }

  @Test
  public void deleteForProjectRemovesEveryMembersConversationsAndMessages() {
    store.saveRecord(record("a", "p1", "alice"));
    store.saveRecord(record("b", "p1", "bob"));
    store.saveRecord(record("c", "p2", "alice"));
    store.saveRecord(record("d", "p2", "bob"));
    for (String id : new String[] {"a", "b", "c", "d"}) {
      store.saveMessages(id, new Conversation(new ArrayList<>(Arrays.asList(ChatMessage.user("hi")))));
    }

    store.deleteForProject("p1");

    assertNull(store.findRecord("a"));
    assertNull(store.findRecord("b"));
    assertTrue(store.findMessages("a").getMessages().isEmpty());
    assertTrue(store.findMessages("b").getMessages().isEmpty());
    // Other projects are untouched, for every owner.
    assertNotNull(store.findRecord("c"));
    assertNotNull(store.findRecord("d"));
    assertEquals(1, store.findMessages("d").getMessages().size());
  }

  @Test
  public void deleteForProjectWithNoConversationsIsANoOp() {
    store.deleteForProject("nothing-here");
  }

  /** Minimal in-memory PersistentStore. */
  private static class MemoryStore<V> implements PersistentStore<V> {
    private final Map<String, V> map = new TreeMap<>();

    @Override
    public PersistentStoreMode getMode() {
      return PersistentStoreMode.PERSISTENT;
    }

    @Override
    public void delete(String key) {
      map.remove(key);
    }

    @Override
    public boolean putIfAbsent(String key, V value) {
      return map.putIfAbsent(key, value) == null;
    }

    @Override
    public Iterator<Map.Entry<String, V>> getRange(int skip, int take) {
      return new ArrayList<>(map.entrySet()).subList(skip, Math.min(map.size(), skip + take)).iterator();
    }

    @Override
    public boolean contains(String key) {
      return map.containsKey(key);
    }

    @Override
    public V get(String key) {
      return map.get(key);
    }

    @Override
    public void put(String key, V value) {
      map.put(key, value);
    }

    @Override
    public Iterator<Map.Entry<String, V>> getAll() {
      return new ArrayList<>(map.entrySet()).iterator();
    }

    @Override
    public void close() {
    }
  }
}
