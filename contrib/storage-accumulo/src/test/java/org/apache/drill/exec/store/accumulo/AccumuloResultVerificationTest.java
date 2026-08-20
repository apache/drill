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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * End-to-end tests that verify the actual values Drill returns from Accumulo,
 * not just the number of rows.
 *
 * <p>Accumulo row keys and values are surfaced to Drill as VARBINARY, so the queries
 * here decode them with {@code CONVERT_FROM(..., 'UTF8')} before comparing against
 * the data written by {@link AccumuloTestUtils}.</p>
 */
public class AccumuloResultVerificationTest extends BaseAccumuloTest {

  // =========================================================================
  // Full table content
  // =========================================================================

  @Test
  public void testAllRowsAndValuesFromTable1() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", "
        + utf8("t.cf.name", "name") + ", "
        + utf8("t.cf.age", "age") + ", "
        + utf8("t.cf.city", "city")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key", "name", "age", "city")
        .baselineValues("row_001", "Alice", "30", "New York")
        .baselineValues("row_002", "Bob", "25", "Los Angeles")
        .baselineValues("row_003", "Charlie", "35", "Chicago")
        .baselineValues("row_004", "Diana", "28", "Houston")
        .baselineValues("row_005", "Eve", "32", "Phoenix")
        .baselineValues("row_006", "Frank", "45", "Philadelphia")
        .baselineValues("row_007", "Grace", "29", "San Antonio")
        .baselineValues("row_008", "Henry", "38", "San Diego")
        .baselineValues("row_009", "Ivy", "26", "Dallas")
        .baselineValues("row_010", "Jack", "41", "San Jose")
        .go();
  }

  @Test
  public void testValuesAcrossMultipleColumnFamilies() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", "
        + utf8("t.personal.first_name", "first_name") + ", "
        + utf8("t.personal.last_name", "last_name") + ", "
        + utf8("t.contact.email", "email") + ", "
        + utf8("t.employment.company", "company") + ", "
        + utf8("t.employment.salary", "salary")
        + fromTable(AccumuloTestUtils.TEST_TABLE_USERS)
        + " WHERE row_key IN ('user_001', 'user_014', 'user_020')"
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key", "first_name", "last_name", "email", "company", "salary")
        .baselineValues("user_001", "John", "Doe", "john.doe@email.com", "Acme Corp", "75000")
        .baselineValues("user_014", "Laura", "White", "laura.w@email.com", "TechCo", "125000")
        .baselineValues("user_020", "Rachel", "Clark", "rachel.c@email.com", "TechCo", "99000")
        .go();
  }

  // =========================================================================
  // Filter pushdown: verify the correct rows come back, not just the count
  // =========================================================================

  @Test
  public void testRowKeyEqualsReturnsMatchingRow() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", " + utf8("t.cf.name", "name")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " WHERE row_key = 'row_003'";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("row_key", "name")
        .baselineValues("row_003", "Charlie")
        .go();
  }

  @Test
  public void testRowKeyRangeReturnsExactRows() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", " + utf8("t.cf.city", "city")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " WHERE row_key >= 'row_003' AND row_key <= 'row_005'"
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key", "city")
        .baselineValues("row_003", "Chicago")
        .baselineValues("row_004", "Houston")
        .baselineValues("row_005", "Phoenix")
        .go();
  }

  @Test
  public void testRowKeyGreaterThanReturnsExactRows() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " WHERE row_key > 'row_007'"
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key")
        .baselineValues("row_008")
        .baselineValues("row_009")
        .baselineValues("row_010")
        .go();
  }

  @Test
  public void testValueFilterReturnsMatchingRows() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_USERS)
        + " WHERE CONVERT_FROM(t.employment.title, 'UTF8') = 'Director'"
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key")
        .baselineValues("user_004")
        .baselineValues("user_014")
        .go();
  }

  @Test
  public void testRowKeyRangeOnLargeTableBoundaries() throws Exception {
    // Exercises a range that spans many rows: verify both endpoints and the count.
    String sql = "SELECT MIN(rk) AS min_rk, MAX(rk) AS max_rk, COUNT(*) AS cnt FROM ("
        + "  SELECT " + utf8("row_key", "rk")
        + " " + fromTable(AccumuloTestUtils.TEST_TABLE_LARGE)
        + "  WHERE row_key >= 'row_0100' AND row_key < 'row_0200')";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("min_rk", "max_rk", "cnt")
        .baselineValues("row_0100", "row_0199", 100L)
        .go();
  }

  // =========================================================================
  // Sort and limit
  // =========================================================================

  @Test
  public void testOrderByRowKeyDescReturnsRowsInOrder() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " ORDER BY row_key DESC LIMIT 3";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key")
        .baselineValues("row_010")
        .baselineValues("row_009")
        .baselineValues("row_008")
        .go();
  }

  @Test
  public void testOrderByWithLimitOnLargeTable() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", " + utf8("t.data.value", "value")
        + fromTable(AccumuloTestUtils.TEST_TABLE_LARGE)
        + " ORDER BY row_key LIMIT 3";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key", "value")
        .baselineValues("row_0001", "1")
        .baselineValues("row_0002", "2")
        .baselineValues("row_0003", "3")
        .go();
  }

  @Test
  public void testLimitReturnsDistinctRowsFromTheTable() throws Exception {
    // A pushed-down limit must return whole, distinct rows rather than repeating or
    // truncating them, so check the returned keys against the full key set.
    String sql = "SELECT " + utf8("row_key", "row_key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_LARGE) + " LIMIT 25";

    List<String> keys = new ArrayList<>();
    for (List<String> row : runAndReadStrings(sql)) {
      keys.add(row.get(0));
    }

    assertEquals(25, keys.size());
    assertEquals("Limit must not return duplicate rows", 25, keys.stream().distinct().count());
    for (String key : keys) {
      assertTrue("Unexpected row key returned: " + key, key.matches("row_0\\d{3}"));
      int index = Integer.parseInt(key.substring(4));
      assertTrue("Row key out of range: " + key, index >= 1 && index <= 1000);
    }
  }

  // =========================================================================
  // Aggregates over Accumulo data
  // =========================================================================

  @Test
  public void testCountStarReturnsRowCount() throws Exception {
    String sql = "SELECT COUNT(*) AS cnt FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE);
    assertEquals(1000L, queryBuilder().sql(sql).singletonLong());
  }

  @Test
  public void testSumOverConvertedValues() throws Exception {
    // Sum of 1..1000
    String sql = "SELECT SUM(CAST(CONVERT_FROM(t.data.value, 'UTF8') AS INT)) AS total"
        + fromTable(AccumuloTestUtils.TEST_TABLE_LARGE);
    assertEquals(500500L, queryBuilder().sql(sql).singletonLong());
  }

  @Test
  public void testGroupByReturnsCorrectCounts() throws Exception {
    String sql = "SELECT CONVERT_FROM(t.employment.company, 'UTF8') AS company, COUNT(*) AS cnt"
        + fromTable(AccumuloTestUtils.TEST_TABLE_USERS)
        + " GROUP BY CONVERT_FROM(t.employment.company, 'UTF8')"
        + " ORDER BY company";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("company", "cnt")
        .baselineValues("Acme Corp", 7L)
        .baselineValues("DataInc", 6L)
        .baselineValues("TechCo", 7L)
        .go();
  }

  // =========================================================================
  // Sparse rows: missing qualifiers must read back as NULL
  // =========================================================================

  @Test
  public void testMissingQualifiersReadBackAsNull() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", "
        + utf8("t.cf.a", "a") + ", " + utf8("t.cf.b", "b") + ", " + utf8("t.cf.c", "c")
        + fromTable(AccumuloTestUtils.TEST_TABLE_SPARSE)
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key", "a", "b", "c")
        .baselineValues("sparse_001", "a1", "b1", "c1")
        .baselineValues("sparse_002", "a2", null, null)
        .baselineValues("sparse_003", null, "b3", null)
        .baselineValues("sparse_004", null, null, "c4")
        .baselineValues("sparse_005", "a5", null, "c5")
        .go();
  }

  @Test
  public void testSparseTableIsNotNullFilter() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_SPARSE)
        + " WHERE t.cf.b IS NOT NULL"
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key")
        .baselineValues("sparse_001")
        .baselineValues("sparse_003")
        .go();
  }

  // =========================================================================
  // Projection: unprojected data must not leak into the result
  // =========================================================================

  @Test
  public void testProjectedFamilyContainsAllQualifiers() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key") + ", "
        + utf8("t.personal.first_name", "first_name") + ", "
        + utf8("t.personal.last_name", "last_name")
        + fromTable(AccumuloTestUtils.TEST_TABLE_USERS)
        + " WHERE row_key = 'user_007'";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("row_key", "first_name", "last_name")
        .baselineValues("user_007", "Edward", "Miller")
        .go();
  }

  @Test
  public void testRowKeyOnlyProjectionReturnsAllKeys() throws Exception {
    String sql = "SELECT " + utf8("row_key", "row_key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_1)
        + " ORDER BY row_key";

    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_key")
        .baselineValues("row_001")
        .baselineValues("row_002")
        .baselineValues("row_003")
        .baselineValues("row_004")
        .baselineValues("row_005")
        .baselineValues("row_006")
        .baselineValues("row_007")
        .baselineValues("row_008")
        .baselineValues("row_009")
        .baselineValues("row_010")
        .go();
  }
}
