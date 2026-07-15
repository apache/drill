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
package org.apache.ranger.authorization.drill.resource;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link DrillAccessResource} construction and getters.
 * Verifies that the resource keys are lowercase and that empty
 * {@code Optional} values do not register a key.
 */
public class DrillAccessResourceTest {

  @Test
  public void constructor_threeArgs_setsDatasourceSchemaTable() {
    DrillAccessResource r = new DrillAccessResource(
        "mysql", Optional.of("shf"), Optional.of("orders"));
    assertEquals("mysql", r.getCatalogName());
    assertEquals("shf", r.getSchema());
    assertEquals("orders", r.getTable());
  }

  @Test
  public void constructor_fourArgs_setsAllKeys() {
    DrillAccessResource r = new DrillAccessResource(
        "mysql", Optional.of("shf"), Optional.of("orders"), Optional.of("amount"));
    assertEquals("mysql", r.getCatalogName());
    assertEquals("shf", r.getSchema());
    assertEquals("orders", r.getTable());
    assertEquals("amount", r.getValue("column"));
  }

  @Test
  public void constructor_emptySchema_doesNotSetSchemaKey() {
    DrillAccessResource r = new DrillAccessResource(
        "mysql", Optional.empty(), Optional.of("orders"));
    assertNull(r.getSchema());
    assertNotNull(r.getCatalogName());
    assertNotNull(r.getTable());
  }

  @Test
  public void constructor_emptyTable_doesNotSetTableKey() {
    DrillAccessResource r = new DrillAccessResource(
        "mysql", Optional.of("shf"), Optional.empty());
    assertEquals("mysql", r.getCatalogName());
    assertEquals("shf", r.getSchema());
    assertNull(r.getTable());
  }

  @Test
  public void getCatalogName_returnsDatasource() {
    DrillAccessResource r = new DrillAccessResource(
        "dfs", Optional.of("tmp"), Optional.of("t1"));
    assertEquals("dfs", r.getCatalogName());
  }

  @Test
  public void getTable_returnsTableValue() {
    DrillAccessResource r = new DrillAccessResource(
        "dfs", Optional.of("tmp"), Optional.of("t1"));
    assertEquals("t1", r.getTable());
  }

  @Test
  public void getSchema_returnsSchemaValue() {
    DrillAccessResource r = new DrillAccessResource(
        "dfs", Optional.of("tmp"), Optional.of("t1"));
    assertEquals("tmp", r.getSchema());
  }

  @Test
  public void resourceKeys_areLowercase() {
    DrillAccessResource r = new DrillAccessResource(
        "mysql", Optional.of("shf"), Optional.of("orders"), Optional.of("amount"));
    // Ranger requires resource keys to be lowercase; verify that lookups
    // with lowercase keys return the registered values.
    assertEquals("mysql", r.getValue("datasource"));
    assertEquals("shf", r.getValue("schema"));
    assertEquals("orders", r.getValue("table"));
    assertEquals("amount", r.getValue("column"));
  }
}
