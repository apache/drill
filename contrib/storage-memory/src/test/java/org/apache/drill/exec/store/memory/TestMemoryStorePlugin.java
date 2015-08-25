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
package org.apache.drill.exec.store.memory;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.store.StoragePluginRegistry;

import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Integration tests for memory store plugin.
 */
public class TestMemoryStorePlugin extends BaseTestQuery {

    private static final Logger logger = LoggerFactory.getLogger(TestMemoryStorePlugin.class);

    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";

    @BeforeClass
    public static void beforeClass() throws Exception {
        MemoryStoragePluginConfig config = new MemoryStoragePluginConfig(new HashMap<String, String>());
        StoragePluginRegistry registry = getDrillbitContext().getStorage();
        registry.createOrUpdate(MemoryStoragePluginConfig.NAME, config, true);
    }

    @Test
    public void testMemoryStorePlugin() throws Exception {

        MemoryStoragePlugin plugin    = (MemoryStoragePlugin) getDrillbitContext().getStorage().getPlugin("memory");
        MemorySchemaFactory factory   = plugin.getFactory();
        MemorySchemaProvider provider = factory.getSchemaProvider();

        provider.createTable(TEST_SCHEMA, TEST_TABLE, SimpleClass.class);
        provider.createTable(TEST_SCHEMA, TEST_TABLE, ComplexClass.class);

        /*
        String sql = "select * from memory.`" + TEST_SCHEMA + "`." + TEST_TABLE;
        testSqlWithResults(sql);
        */
    }

    public static class SimpleClass {
        int    x;
        double y;
        String z;
    }

    public static class ComplexClass {
        int    xxx;
        double yyy;
        String zzz;
        SimpleClass a;
    }
}
