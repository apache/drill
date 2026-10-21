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
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.store.sys.PersistentStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RunClaimsTest {

  @SuppressWarnings("unchecked")
  private static PersistentStore<RunClaims.Claim> store(Map<String, RunClaims.Claim> map) {
    PersistentStore<RunClaims.Claim> s = mock(PersistentStore.class);
    when(s.putIfAbsent(anyString(), any())).thenAnswer(i -> map.putIfAbsent(i.getArgument(0), i.getArgument(1)) == null);
    when(s.getAll()).thenAnswer(i -> map.entrySet().iterator());
    doAnswer(i -> map.remove(i.<String>getArgument(0))).when(s).delete(anyString());
    return s;
  }

  @Test
  public void onlyFirstDrillbitWinsASlot() {
    Map<String, RunClaims.Claim> map = new ConcurrentHashMap<>();
    RunClaims a = new RunClaims(store(map));
    RunClaims b = new RunClaims(store(map));
    assertTrue(a.claim("wf_1792569600000", "node-a"));
    assertFalse(b.claim("wf_1792569600000", "node-b"));
    assertTrue(b.claim("wf_1792656000000", "node-b"));
    assertEquals("node-a", map.get("wf_1792569600000").drillbit);
  }

  @Test
  public void claimKeyIsSanitizedForLocalAndZkStores() {
    Map<String, RunClaims.Claim> map = new ConcurrentHashMap<>();
    RunClaims c = new RunClaims(store(map));
    assertTrue(c.claim("wf/1:2026-10-21T08:00:00Z", "n"));
    assertEquals(Set.of("wf_1_2026-10-21T08_00_00Z"), map.keySet());
  }

  @Test
  public void pruneRemovesOldClaims() {
    Map<String, RunClaims.Claim> map = new ConcurrentHashMap<>();
    RunClaims c = new RunClaims(store(map));
    c.claim("old", "n");
    map.get("old").claimedAt = "2026-01-01T00:00:00Z";
    c.claim("new", "n");
    c.prune(Instant.parse("2026-06-01T00:00:00Z"));
    assertEquals(Set.of("new"), map.keySet());
  }
}
