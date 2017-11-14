/**
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
package org.apache.drill.exec.store.sys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

public class PStoreTestUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PStoreTestUtil.class);

  public static void test(PersistentStoreProvider provider) throws Exception{
    PersistentStore<String> store = provider.getOrCreateStore(PersistentStoreConfig.newJacksonBuilder(new ObjectMapper(), String.class).name("sys.test").build());
    String[] keys = {"first", "second"};
    String[] values = {"value1", "value2"};
    Map<String, String> expectedMap = Maps.newHashMap();

    for(int i =0; i < keys.length; i++){
      expectedMap.put(keys[i], values[i]);
      store.put(keys[i], values[i]);
    }

    // Wait for store caches to update, this is necessary because ZookeeperClient caches can update asynchronously in some cases.
    waitForNumProps(store, keys.length);

    {
      Iterator<Map.Entry<String, String>> iter = store.getAll();
      for(int i =0; i < keys.length; i++){
        Entry<String, String> e = iter.next();
        assertTrue(expectedMap.containsKey(e.getKey()));
        assertEquals(expectedMap.get(e.getKey()), e.getValue());
      }

      assertFalse(iter.hasNext());
    }

    {
      Iterator<Map.Entry<String, String>> iter = store.getAll();
      while(iter.hasNext()){
        final String key = iter.next().getKey();
        store.delete(key);
      }
    }

    // Wait for store caches to update, this is necessary because ZookeeperClient caches can update asynchronously in some cases.
    waitForNumProps(store, 0);
    assertFalse(store.getAll().hasNext());
  }

  private static void waitForNumProps(PersistentStore store, int expected) throws InterruptedException {
    for (int numProps = getNumProps(store);
         numProps < expected;
         numProps = getNumProps(store)) {
      Thread.sleep(100L);
    }
  }

  private static int getNumProps(PersistentStore store) {
    Iterator<Map.Entry<String, String>> iter = store.getAll();

    int numProps = 0;

    while (iter.hasNext()) {
      iter.next();
      numProps++;
    }

    return numProps;
  }
}
