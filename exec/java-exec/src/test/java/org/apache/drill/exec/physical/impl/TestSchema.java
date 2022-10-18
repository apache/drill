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
package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.mock.MockBreakageStorage;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterMockStorageFixture;
import org.apache.drill.test.DrillTest;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSchema extends DrillTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  private static ClusterMockStorageFixture cluster;
  private static ClientFixture client;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = ClusterFixture.builder(dirTestWatcher).buildCustomMockStorage();
    boolean breakRegisterSchema = true;
    cluster.insertMockStorage("mock_broken", breakRegisterSchema);
    cluster.insertMockStorage("mock_good", !breakRegisterSchema);
    client = cluster.clientFixture();
  }

  @Test (expected = Exception.class)
  public void testQueryBrokenRegSchema() throws Exception {
    MockBreakageStorage mbs = (MockBreakageStorage) cluster.storageRegistry().getPlugin("mock_broken");
    String sql = "SELECT id_i, name_s10 FROM `mock_broken`.`employees_5`";

    client.alterSystem(ExecConstants.STORAGE_PLUGIN_AUTO_DISABLE, false);
    client.alterSystem(ExecConstants.STORAGE_PLUGIN_RETRY_ATTEMPTS, 2);
    client.alterSystem(ExecConstants.STORAGE_PLUGIN_RETRY_DELAY, 0);

    mbs.setBreakRegister(true);
    try {
      client.queryBuilder().sql(sql).run();
    } catch (Exception ex) {
      assertTrue(
        ex.getMessage()
          .split(System.lineSeparator())[0]
          .matches("^RESOURCE ERROR: Failed to load schema.*")
      );

      assertEquals(3, mbs.registerAttemptCount);
      // The plugin should still be enabled because we set auto_disable to false.
      assertTrue(cluster.storageRegistry().availablePlugins().contains("mock_broken"));
      throw ex;
    } finally {
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_AUTO_DISABLE);
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_RETRY_ATTEMPTS);
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_RETRY_DELAY);
    }
  }

  @Test (expected = Exception.class)
  public void testAutoDisableBrokenRegSchema() throws Exception {
    MockBreakageStorage mbs = (MockBreakageStorage) cluster.storageRegistry().getPlugin("mock_broken");
    String sql = "SELECT id_i, name_s10 FROM `mock_broken`.`employees_5`";

    client.alterSystem(ExecConstants.STORAGE_PLUGIN_AUTO_DISABLE, true);
    client.alterSystem(ExecConstants.STORAGE_PLUGIN_RETRY_ATTEMPTS, 0);

    mbs.setBreakRegister(true);
    try {
      client.queryBuilder().sql(sql).run();
    } catch (Exception ex) {
      assertTrue(
        ex.getMessage()
          .split(System.lineSeparator())[0]
          .matches("^PLUGIN ERROR: Failed to load schema.*")
      );

      // The plugin should no longer be enabled because we set auto_disable to false.
      assertFalse(cluster.storageRegistry().availablePlugins().contains("mock_broken"));
      throw ex;
    } finally {
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_AUTO_DISABLE);
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_RETRY_ATTEMPTS);
      cluster.storageRegistry().setEnabled("mock_broken", true);
    }
  }

  @Test
  public void testQueryGoodStorage() throws Exception {
    String sql = "SELECT id_i, name_s10 FROM `mock_good`.`employees_5`";
    client.queryBuilder().sql(sql).run();
  }

  @Test
  public void testQueryGoodStorageWithDefaultSchema() throws Exception {
    String use_dfs = "use dfs.tmp";
    client.queryBuilder().sql(use_dfs).run();
    String sql = "SELECT id_i, name_s10 FROM `mock_good`.`employees_5`";
    client.queryBuilder().sql(sql).run();
  }

  @Test (expected = Exception.class)
  public void testUseBrokenStorage() throws Exception {
    MockBreakageStorage mbs = (MockBreakageStorage) cluster.storageRegistry().getPlugin("mock_broken");
    client.alterSystem(ExecConstants.STORAGE_PLUGIN_AUTO_DISABLE, false);
    client.alterSystem(ExecConstants.STORAGE_PLUGIN_RETRY_ATTEMPTS, 0);

    mbs.setBreakRegister(true);
    try {
      String use_dfs = "use mock_broken";
      client.queryBuilder().sql(use_dfs).run();
    } catch(Exception ex) {
      assertTrue(ex.getMessage().contains("RESOURCE ERROR: Failed to load schema"));
      throw ex;
    } finally {
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_AUTO_DISABLE);
      client.resetSystem(ExecConstants.STORAGE_PLUGIN_RETRY_ATTEMPTS);
    }
  }

  @Test
  public void testLocal() throws Exception {
    try {
      client.queryBuilder()
          .sql("create table dfs.tmp.t1 as select 1 as c1")
          .run();

      client.queryBuilder()
          .sql("create table dfs.tmp.t2 as select 1 as c1")
          .run();

      client.testBuilder()
          .sqlQuery("select a.c1 from dfs.tmp.`t1` a\n" +
            "join `dfs.tmp`.`t2` b ON b.c1 = a.c1\n")
          .unOrdered()
          .baselineColumns("c1")
          .baselineValues(1)
          .go();
    } finally {
      client.queryBuilder()
          .sql("drop table if exists `dfs.tmp`.t1")
          .run();

      client.queryBuilder()
          .sql("drop table if exists dfs.tmp.t2")
          .run();
    }
  }

}
