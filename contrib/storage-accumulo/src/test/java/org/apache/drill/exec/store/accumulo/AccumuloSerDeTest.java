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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.base.FragmentLeaf;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.store.accumulo.AccumuloScanSpec.AccumuloColumnSpec;
import org.junit.Test;

/**
 * Serialization/deserialization tests for the Accumulo physical operators.
 *
 * <p>Drill serializes physical operators to JSON when it distributes fragments to
 * other Drillbits, so anything that survives planning must survive a JSON round trip.
 * These tests cover both the whole-plan path (plan the query, serialize it, then submit
 * the serialized plan and check the results) and a direct round trip of
 * {@link AccumuloSubScan} through Drill's {@link PhysicalPlanReader}.</p>
 */
public class AccumuloSerDeTest extends BaseAccumuloTest {

  // =========================================================================
  // Whole-plan round trip: plan -> JSON -> execute -> verify results
  // =========================================================================

  @Test
  public void testSerDeSelectStar() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1);
    String plan = queryBuilder().sql(sql).explainJson();

    assertTrue("Plan should contain the Accumulo scan", plan.contains("accumulo-scan"));
    assertEquals(10, queryBuilder().physical(plan).run().recordCount());
  }

  @Test
  public void testSerDePreservesValues() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", " + utf8("t.cf.name", "name")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " WHERE row_key >= 'row_008' ORDER BY row_key";
    String plan = queryBuilder().sql(sql).explainJson();

    assertEquals(
        Arrays.asList(
            Arrays.asList("row_008", "Henry"),
            Arrays.asList("row_009", "Ivy"),
            Arrays.asList("row_010", "Jack")),
        readStringsFromPlan(plan));
  }

  @Test
  public void testSerDePreservesRowRangePushdown() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE)
        + " WHERE row_key >= 'row_0100' AND row_key < 'row_0200'";
    String plan = queryBuilder().sql(sql).explainJson();

    // The deserialized plan must scan the same range, not the whole table.
    assertEquals(100, queryBuilder().physical(plan).run().recordCount());
  }

  @Test
  public void testSerDePreservesLimitPushdown() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE) + " LIMIT 17";
    String plan = queryBuilder().sql(sql).explainJson();

    assertEquals(17, queryBuilder().physical(plan).run().recordCount());
  }

  @Test
  public void testSerDeAggregate() throws Exception {
    String sql = "SELECT COUNT(*) FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS);
    String plan = queryBuilder().sql(sql).explainJson();

    assertEquals(20L, queryBuilder().physical(plan).singletonLong());
  }

  @Test
  public void testFragmentSerDe() throws Exception {
    // A slice target of 1 forces the plan to be split into fragments, which are
    // serialized individually before being handed to the executor.
    client.alterSession(ExecConstants.SLICE_TARGET, 1);
    try {
      String sql = "SELECT CONVERT_FROM(t.employment.company, 'UTF8') AS company, COUNT(*) AS cnt"
          + fromTable(AccumuloTestUtils.TEST_TABLE_USERS)
          + " GROUP BY CONVERT_FROM(t.employment.company, 'UTF8')";
      String plan = queryBuilder().sql(sql).explainJson();

      List<List<String>> rows = readStringsFromPlan(plan);
      rows.sort(Comparator.comparing(row -> row.get(0)));
      assertEquals(
          Arrays.asList(
              Arrays.asList("Acme Corp", "7"),
              Arrays.asList("DataInc", "6"),
              Arrays.asList("TechCo", "7")),
          rows);
    } finally {
      client.resetSession(ExecConstants.SLICE_TARGET);
    }
  }

  // =========================================================================
  // Direct operator round trip
  // =========================================================================

  @Test
  public void testSubScanSerDeRoundTrip() throws Exception {
    AccumuloScanSpec scanSpec = new AccumuloScanSpec(
        AccumuloTestUtils.TEST_TABLE_1,
        "row_002".getBytes(StandardCharsets.UTF_8),
        "row_008".getBytes(StandardCharsets.UTF_8),
        true,
        false,
        Collections.singletonList(new AccumuloColumnSpec("cf", "name", "cf.name")),
        "cf.age > 30",
        25,
        true,
        true);

    List<SchemaPath> columns = Arrays.asList(
        SchemaPath.getSimplePath("row_key"),
        SchemaPath.getCompoundPath("cf", "name"));

    AccumuloSubScan subScan = new AccumuloSubScan(
        "testUser", storagePlugin, scanSpec, columns, 25, null);

    AccumuloSubScan deserialized = roundTrip(subScan);

    assertEquals("testUser", deserialized.getUserName());
    assertEquals(25, deserialized.getMaxRecords());
    assertEquals(columns, deserialized.getColumns());
    assertEquals(storagePluginConfig, deserialized.getStoragePluginConfig());
    assertNull(deserialized.getDelegationTokenInfo());

    AccumuloScanSpec deserializedSpec = deserialized.getScanSpec();
    assertEquals(scanSpec, deserializedSpec);
    assertEquals(AccumuloTestUtils.TEST_TABLE_1, deserializedSpec.getTableName());
    assertArrayEquals("row_002".getBytes(StandardCharsets.UTF_8), deserializedSpec.getStartRow());
    assertArrayEquals("row_008".getBytes(StandardCharsets.UTF_8), deserializedSpec.getStopRow());
    assertTrue(deserializedSpec.isStartRowInclusive());
    assertFalse(deserializedSpec.isStopRowInclusive());
    assertEquals("cf.age > 30", deserializedSpec.getFilterExpression());
    assertEquals(Integer.valueOf(25), deserializedSpec.getLimit());
    assertTrue(deserializedSpec.isUseSortedScanner());
    assertTrue(deserializedSpec.isSortDescending());
    assertEquals(1, deserializedSpec.getColumns().size());
    assertEquals("cf", deserializedSpec.getColumns().get(0).getColumnFamily());
    assertEquals("name", deserializedSpec.getColumns().get(0).getColumnQualifier());
  }

  @Test
  public void testSubScanSerDeRoundTripWithDefaults() throws Exception {
    AccumuloSubScan subScan = new AccumuloSubScan(
        "testUser",
        storagePlugin,
        new AccumuloScanSpec(AccumuloTestUtils.TEST_TABLE_USERS),
        null);

    AccumuloSubScan deserialized = roundTrip(subScan);

    assertEquals(-1, deserialized.getMaxRecords());
    assertNull(deserialized.getColumns());
    assertNull(deserialized.getDelegationTokenInfo());
    assertEquals(AccumuloTestUtils.TEST_TABLE_USERS, deserialized.getScanSpec().getTableName());
    assertNull(deserialized.getScanSpec().getStartRow());
    assertNull(deserialized.getScanSpec().getStopRow());
  }

  @Test
  public void testSubScanSerDeRoundTripWithDelegationToken() throws Exception {
    String serializedToken = Base64.getEncoder()
        .encodeToString("fake-token-bytes".getBytes(StandardCharsets.UTF_8));
    long creationTime = System.currentTimeMillis();
    DelegationTokenInfo tokenInfo = new DelegationTokenInfo(
        "alice",
        serializedToken,
        "org.apache.accumulo.core.client.security.tokens.DelegationTokenImpl",
        creationTime);

    AccumuloSubScan subScan = new AccumuloSubScan(
        "alice",
        storagePlugin,
        new AccumuloScanSpec(AccumuloTestUtils.TEST_TABLE_1),
        null,
        -1,
        tokenInfo);

    AccumuloSubScan deserialized = roundTrip(subScan);

    assertTrue(deserialized.hasDelegationToken());
    DelegationTokenInfo deserializedToken = deserialized.getDelegationTokenInfo();
    assertNotNull(deserializedToken);
    assertEquals("alice", deserializedToken.getUserName());
    assertEquals(serializedToken, deserializedToken.getSerializedToken());
    assertEquals("org.apache.accumulo.core.client.security.tokens.DelegationTokenImpl",
        deserializedToken.getTokenClassName());
    assertEquals(creationTime, deserializedToken.getCreationTime());
  }

  /**
   * Submits a serialized physical plan and returns the results as rows of strings.
   */
  private List<List<String>> readStringsFromPlan(String plan) throws Exception {
    return readStrings(queryBuilder().physical(plan).rowSetIterator());
  }

  /**
   * Writes the operator to JSON with Drill's physical plan mapper and reads it back,
   * which is exactly what happens when a fragment is shipped to another Drillbit.
   */
  private AccumuloSubScan roundTrip(AccumuloSubScan subScan) throws Exception {
    PhysicalPlanReader reader = cluster.drillbit().getContext().getPlanReader();
    String json = reader.writeJson(subScan);
    FragmentLeaf leaf = reader.readFragmentLeaf(json);
    assertTrue("Expected an AccumuloSubScan, got " + leaf.getClass().getName(),
        leaf instanceof AccumuloSubScan);
    return (AccumuloSubScan) leaf;
  }
}
