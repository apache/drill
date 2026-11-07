/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.security;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for {@link TableAccessResource#resolve(List)}: the single
 * mapping from a Calcite qualified name to the (datasource, schema, table)
 * access-check resource. Both table-level (DrillCalciteCatalogReader) and
 * column-level (ColumnAccessChecker) checks go through it, so these tests
 * pin the exact resource each check will address.
 */
public class TableAccessResourceTest extends BaseTest {

  // ========================================================================
  // Three or more segments: datasource.schema.table
  // ========================================================================

  @Test
  public void resolve_threeSegments_splitsDataSourceSchemaTable() {
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("mysql", "shf", "orders"));
    assertEquals("mysql", r.getDataSource());
    assertEquals("shf", r.getSchemaPath());
    assertEquals("orders", r.getTable());
  }

  @Test
  public void resolve_nestedSchema_joinsMiddleSegments() {
    // Nested schema path: [dfs, tmp, sub, orders] -> schema "tmp.sub"
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("dfs", "tmp", "sub", "orders"));
    assertEquals("dfs", r.getDataSource());
    assertEquals("tmp.sub", r.getSchemaPath());
    assertEquals("orders", r.getTable());
  }

  @Test
  public void resolve_schema_excludesDataSourcePrefix() {
    // The schema MUST NOT include the datasource prefix, otherwise policy
    // matching fails (policy has schema=shf but request would send mysql.shf).
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("mysql", "shf", "orders"));
    assertEquals("shf", r.getSchemaPath());
  }

  // ========================================================================
  // Two segments: datasource.table (backend without schema concept)
  // ========================================================================

  @Test
  public void resolve_twoSegments_synthesizesDefaultSchema_forDfs() {
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("dfs", "orders"));
    assertEquals("dfs", r.getDataSource());
    assertEquals("default", r.getSchemaPath());
    assertEquals("orders", r.getTable());
  }

  @Test
  public void resolve_twoSegments_synthesizesDefaultSchema_forCp() {
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("cp", "employee.json"));
    assertEquals("cp", r.getDataSource());
    assertEquals("default", r.getSchemaPath());
  }

  @Test
  public void resolve_twoSegments_defaultSchemaIsCaseInsensitive() {
    // "DFS" lowercases to "dfs" which hits the explicit case -> "default"
    assertEquals("default",
        TableAccessResource.resolve(Arrays.asList("DFS", "orders")).getSchemaPath());
  }

  @Test
  public void resolve_twoSegments_unknownPlugin_usesPluginNameAsSchema() {
    // No explicit default-schema mapping for "mysql": the plugin name itself
    // becomes the default schema namespace (preserving case).
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("mysql", "orders"));
    assertEquals("mysql", r.getDataSource());
    assertEquals("mysql", r.getSchemaPath());
    assertEquals("MySql",
        TableAccessResource.resolve(Arrays.asList("MySql", "orders")).getSchemaPath());
  }

  // ========================================================================
  // Single segment: root-level table
  // ========================================================================

  @Test
  public void resolve_singleSegment_usesTableAsDataSourceNamespace() {
    TableAccessResource r = TableAccessResource.resolve(Collections.singletonList("orders"));
    assertEquals("orders", r.getDataSource());
    assertEquals("orders", r.getSchemaPath());
    assertEquals("orders", r.getTable());
  }

  // ========================================================================
  // Invalid input
  // ========================================================================

  @Test
  public void resolve_nullQualifiedName_throws() {
    assertThrows(NullPointerException.class, () -> TableAccessResource.resolve(null));
  }

  @Test
  public void resolve_emptyQualifiedName_throws() {
    List<String> empty = Collections.emptyList();
    assertThrows(IllegalArgumentException.class, () -> TableAccessResource.resolve(empty));
  }

  // ========================================================================
  // toString: used in permission-error messages
  // ========================================================================

  @Test
  public void toString_rendersDottedPath() {
    TableAccessResource r = TableAccessResource.resolve(Arrays.asList("mysql", "shf", "orders"));
    assertEquals("mysql.shf.orders", r.toString());
  }
}
