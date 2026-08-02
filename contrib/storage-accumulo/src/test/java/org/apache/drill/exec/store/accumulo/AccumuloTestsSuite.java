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
package org.apache.drill.exec.store.accumulo;

import org.apache.drill.exec.store.accumulo.schema.AccumuloColumnTypeTest;
import org.apache.drill.exec.store.accumulo.schema.TableSchemaTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for Accumulo storage plugin.
 *
 * <p>This suite includes all unit tests for the Accumulo plugin components.
 * Integration tests requiring MiniAccumuloCluster will be added in later phases.</p>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Phase 1: Core components
    AccumuloStoragePluginConfigTest.class,
    AccumuloScanSpecTest.class,
    // Phase 2: Schema discovery
    AccumuloColumnTypeTest.class,
    TableSchemaTest.class,
    // Phase 3: Basic scan capability
    DrillAccumuloConstantsTest.class,
    AccumuloTypeConverterTest.class,
    // Phase 4: Filter pushdown
    AccumuloFilterBuilderTest.class,
    // Phase 5: Projection pushdown
    AccumuloProjectionPushdownTest.class,
    // Phase 6: Limit pushdown
    AccumuloLimitPushdownTest.class,
    // Phase 7: Sort pushdown
    AccumuloSortPushdownTest.class,
    // Phase 8: Kerberos authentication
    AccumuloKerberosConfigTest.class,
    DelegationTokenInfoTest.class
})
public class AccumuloTestsSuite {
  // Test suite - no implementation needed
}
