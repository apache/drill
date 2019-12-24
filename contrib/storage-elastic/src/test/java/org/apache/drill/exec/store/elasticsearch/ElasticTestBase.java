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

package org.apache.drill.exec.store.elasticsearch;


import org.apache.drill.PlanTestBase;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.List;

public class ElasticTestBase extends PlanTestBase {

    private static ElasticSearchStoragePlugin storagePlugin;
    private static ElasticSearchPluginConfig storagePluginConfig;

    @BeforeClass
    public static void setupBeforeClass() throws ExecutionSetupException {
        initElasticSearchPlugin();
    }

    private static void initElasticSearchPlugin() throws ExecutionSetupException {
        final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
        storagePlugin = (ElasticSearchStoragePlugin) pluginRegistry.getPlugin(ElasticSearchPluginConfig.NAME);
        storagePluginConfig = storagePlugin.getConfig();
        storagePluginConfig.setEnabled(true);
        pluginRegistry.createOrUpdate(ElasticSearchPluginConfig.NAME, storagePluginConfig, true);
    }

    public void runElasticSearchSQLVerifyCount(String sql, int expectedRowCount)
            throws Exception {
        List<QueryDataBatch> results = runElasticSearchSQLWithResults(sql);
        printResultAndVerifyRowCount(results, expectedRowCount);
    }

    public List<QueryDataBatch> runElasticSearchSQLWithResults(String sql)
            throws Exception {
        return testSqlWithResults(sql);
    }

    public void printResultAndVerifyRowCount(List<QueryDataBatch> results,
                                             int expectedRowCount) throws SchemaChangeException {
        int rowCount = printResult(results);
        if (expectedRowCount != -1) {
            Assert.assertEquals(expectedRowCount, rowCount);
        }
    }

}
