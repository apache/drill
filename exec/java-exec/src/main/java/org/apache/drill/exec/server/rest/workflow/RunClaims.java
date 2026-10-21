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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.drill.exec.store.sys.PersistentStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Ensures one Drillbit runs each scheduled slot. Backed by putIfAbsent, which is an
 * atomic ZooKeeper create in clustered mode.
 */
public class RunClaims {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Claim {
    public String drillbit;
    public String claimedAt;
  }

  private final PersistentStore<Claim> store;

  public RunClaims(PersistentStore<Claim> store) {
    this.store = store;
  }

  public boolean claim(String key, String drillbit) {
    Claim c = new Claim();
    c.drillbit = drillbit;
    c.claimedAt = Instant.now().toString();
    // LocalPersistentStore rejects ':' and ZooKeeper rejects '/', so keep keys to a safe alphabet.
    return store.putIfAbsent(key.replaceAll("[^A-Za-z0-9_.-]", "_"), c);
  }

  public void prune(Instant olderThan) {
    List<String> stale = new ArrayList<>();
    Iterator<Map.Entry<String, Claim>> it = store.getAll();
    while (it.hasNext()) {
      Map.Entry<String, Claim> e = it.next();
      String at = e.getValue() == null ? null : e.getValue().claimedAt;
      if (at == null || Instant.parse(at).isBefore(olderThan)) {
        stale.add(e.getKey());
      }
    }
    stale.forEach(store::delete);
  }
}
