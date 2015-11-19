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
package org.apache.drill.store.kudu;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.store.kudu.config.KuduPStoreProvider;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKuduTableProvider extends BaseKuduTest {

  private static KuduPStoreProvider provider;

  @BeforeClass // mask Kudu cluster start function
  public static void setUpBeforeTestKuduTableProvider() throws Exception {
    provider = new KuduPStoreProvider(storagePluginConfig.getKuduConf(), "drill_store");
    provider.start();
  }

  @Test
  public void testTableProvider() throws IOException {
    LogicalPlanPersistence lp = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(config);
    PStore<String> kuduStore = provider.getStore(PStoreConfig.newJacksonBuilder(lp.getMapper(), String.class).name("kudu").build());
    kuduStore.put("", "v0");
    kuduStore.put("k1", "v1");
    kuduStore.put("k2", "v2");
    kuduStore.put("k3", "v3");
    kuduStore.put("k4", "v4");
    kuduStore.put("k5", "v5");
    kuduStore.put(".test", "testValue");

    assertEquals("v0", kuduStore.get(""));
    assertEquals("testValue", kuduStore.get(".test"));

    int rowCount = 0;
    for (Entry<String, String> entry : kuduStore) {
      rowCount++;
      System.out.println(entry.getKey() + "=" + entry.getValue());
    }
    assertEquals(7, rowCount);

    PStore<String> kuduTestStore = provider.getStore(PStoreConfig.newJacksonBuilder(lp.getMapper(), String.class).name("kudu.test").build());
    kuduTestStore.put("", "v0");
    kuduTestStore.put("k1", "v1");
    kuduTestStore.put("k2", "v2");
    kuduTestStore.put("k3", "v3");
    kuduTestStore.put("k4", "v4");
    kuduTestStore.put(".test", "testValue");

    assertEquals("v0", kuduStore.get(""));
    assertEquals("testValue", kuduStore.get(".test"));

    rowCount = 0;
    for (Entry<String, String> entry : kuduTestStore) {
      rowCount++;
      System.out.println(entry.getKey() + "=" + entry.getValue());
    }
    assertEquals(6, rowCount);
  }

  @AfterClass
  public static void tearDownTestKuduTableProvider() {
    if (provider != null) {
      provider.close();
    }
  }
}
