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
package org.apache.drill.exec.planner.sql.conversion;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DrillCalciteCatalogReader#getDefaultSchemaByDataSource}.
 * The method is package-private static; this test class must live in the same
 * package to invoke it directly.
 *
 * <p>The datasource argument is always a non-null storage plugin name obtained
 * from a table's qualified name, so null/empty inputs are not exercised here.
 * The switch matches on lowercased input: explicit cases ({@code "dfs"}, {@code cp})
 * return {@code "default"}; the {@code default} branch returns the ORIGINAL
 * datasource name (preserving case). These tests pin that behavior so future
 * additions of explicit cases are intentional.</p>
 */
public class DrillCalciteCatalogReaderTest extends BaseTest {

  @Test
  public void getDefaultSchemaByDataSource_returnsDefault_forDfs() {
    assertEquals("default", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("dfs"));
  }

  @Test
  public void getDefaultSchemaByDataSource_returnsDefault_forCp() {
    assertEquals("default", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("cp"));
  }

  @Test
  public void getDefaultSchemaByDataSource_returnsDataSource_forUnknownPlugin() {
    // No explicit case for "mysql" — falls through to default branch which
    // returns the input unchanged.
    assertEquals("mysql", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("mysql"));
  }

  @Test
  public void getDefaultSchemaByDataSource_isCaseInsensitive() {
    // The switch matches on lowercased input. "DFS" lowercases to "dfs" which
    // hits the explicit case and returns "default"; "MySql" lowercases to
    // "mysql" which falls through to default and returns the original input.
    assertEquals("default", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("DFS"));
    assertEquals("MySql", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("MySql"));
  }

  @Test
  public void getDefaultSchemaByDataSource_returnsDataSource_forCustomName() {
    assertEquals("my_plugin", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("my_plugin"));
    assertEquals("hbase", DrillCalciteCatalogReader.getDefaultSchemaByDataSource("hbase"));
  }
}
