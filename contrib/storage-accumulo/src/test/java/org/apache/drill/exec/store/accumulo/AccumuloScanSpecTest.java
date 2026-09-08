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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.accumulo.AccumuloScanSpec.AccumuloColumnSpec;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for AccumuloScanSpec.
 */
public class AccumuloScanSpecTest extends BaseTest {

  @Test
  public void testSimpleConstruction() {
    AccumuloScanSpec spec = new AccumuloScanSpec("test_table");

    assertEquals("test_table", spec.getTableName());
    assertNull(spec.getStartRow());
    assertNull(spec.getStopRow());
    assertTrue(spec.isStartRowInclusive());
    assertFalse(spec.isStopRowInclusive());
    assertNull(spec.getColumns());
    assertNull(spec.getFilterExpression());
    assertNull(spec.getLimit());
    assertFalse(spec.isUseSortedScanner());
  }

  @Test
  public void testFullConstruction() {
    byte[] startRow = "row_001".getBytes();
    byte[] stopRow = "row_999".getBytes();
    List<AccumuloColumnSpec> columns = Arrays.asList(
        new AccumuloColumnSpec("cf1", "name", "name"),
        new AccumuloColumnSpec("cf1", "age", "age")
    );

    AccumuloScanSpec spec = new AccumuloScanSpec(
        "test_table",
        startRow,
        stopRow,
        true,
        false,
        columns,
        "age > 30",
        100,
        true,
        false
    );

    assertEquals("test_table", spec.getTableName());
    assertArrayEquals(startRow, spec.getStartRow());
    assertArrayEquals(stopRow, spec.getStopRow());
    assertTrue(spec.isStartRowInclusive());
    assertFalse(spec.isStopRowInclusive());
    assertEquals(2, spec.getColumns().size());
    assertEquals("age > 30", spec.getFilterExpression());
    assertEquals(Integer.valueOf(100), spec.getLimit());
    assertTrue(spec.isUseSortedScanner());
    assertFalse(spec.isSortDescending());
  }

  @Test
  public void testHelperMethods() {
    AccumuloScanSpec specNoExtras = new AccumuloScanSpec("table1");
    assertFalse(specNoExtras.hasFilter());
    assertFalse(specNoExtras.hasLimit());
    assertFalse(specNoExtras.hasRowRange());

    AccumuloScanSpec specWithFilter = new AccumuloScanSpec(
        "table2", null, null, true, false, null, "col = 'value'", null, false, false);
    assertTrue(specWithFilter.hasFilter());
    assertFalse(specWithFilter.hasLimit());
    assertFalse(specWithFilter.hasRowRange());

    AccumuloScanSpec specWithLimit = new AccumuloScanSpec(
        "table3", null, null, true, false, null, null, 50, false, false);
    assertFalse(specWithLimit.hasFilter());
    assertTrue(specWithLimit.hasLimit());
    assertFalse(specWithLimit.hasRowRange());

    AccumuloScanSpec specWithRange = new AccumuloScanSpec(
        "table4", "start".getBytes(), "stop".getBytes(), true, false, null, null, null, false, false);
    assertFalse(specWithRange.hasFilter());
    assertFalse(specWithRange.hasLimit());
    assertTrue(specWithRange.hasRowRange());
  }

  @Test
  public void testWithMethods() {
    AccumuloScanSpec original = new AccumuloScanSpec("test_table");

    AccumuloScanSpec withFilter = original.withFilter("status = 'active'");
    assertEquals("status = 'active'", withFilter.getFilterExpression());
    assertNull(original.getFilterExpression()); // Original unchanged

    AccumuloScanSpec withLimit = original.withLimit(100);
    assertEquals(Integer.valueOf(100), withLimit.getLimit());
    assertNull(original.getLimit()); // Original unchanged

    AccumuloScanSpec withSorted = original.withSortedScanner(true);
    assertTrue(withSorted.isUseSortedScanner());
    assertFalse(original.isUseSortedScanner()); // Original unchanged
  }

  @Test
  public void testEquality() {
    AccumuloScanSpec spec1 = new AccumuloScanSpec("table1");
    AccumuloScanSpec spec2 = new AccumuloScanSpec("table1");
    AccumuloScanSpec spec3 = new AccumuloScanSpec("table2");

    assertEquals(spec1, spec2);
    assertEquals(spec1.hashCode(), spec2.hashCode());
    assertNotEquals(spec1, spec3);
  }

  @Test
  public void testJsonSerialization() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    AccumuloScanSpec spec = new AccumuloScanSpec(
        "test_table",
        "start".getBytes(),
        "stop".getBytes(),
        true,
        false,
        Arrays.asList(new AccumuloColumnSpec("cf1", "col1", "col1")),
        "col1 > 10",
        50,
        true,
        true
    );

    String json = mapper.writeValueAsString(spec);
    assertNotNull(json);
    assertTrue(json.contains("test_table"));
    assertTrue(json.contains("col1 > 10"));

    AccumuloScanSpec deserialized = mapper.readValue(json, AccumuloScanSpec.class);
    assertEquals(spec.getTableName(), deserialized.getTableName());
    assertEquals(spec.getFilterExpression(), deserialized.getFilterExpression());
    assertEquals(spec.getLimit(), deserialized.getLimit());
    assertEquals(spec.isUseSortedScanner(), deserialized.isUseSortedScanner());
    assertEquals(spec.isSortDescending(), deserialized.isSortDescending());
  }

  @Test
  public void testColumnSpec() {
    AccumuloColumnSpec colSpec = new AccumuloColumnSpec("family1", "qualifier1", "drill_column");

    assertEquals("family1", colSpec.getColumnFamily());
    assertEquals("qualifier1", colSpec.getColumnQualifier());
    assertEquals("drill_column", colSpec.getDrillColumnName());

    AccumuloColumnSpec colSpec2 = new AccumuloColumnSpec("family1", "qualifier1", "drill_column");
    assertEquals(colSpec, colSpec2);
    assertEquals(colSpec.hashCode(), colSpec2.hashCode());
  }

  @Test
  public void testDigest() {
    AccumuloScanSpec spec = new AccumuloScanSpec("my_table");
    String digest = spec.digest();

    assertNotNull(digest);
    assertTrue(digest.contains("my_table"));
  }
}
