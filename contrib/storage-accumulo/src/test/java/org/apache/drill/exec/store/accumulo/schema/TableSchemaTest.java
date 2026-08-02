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
package org.apache.drill.exec.store.accumulo.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for ColumnDef and TableSchema.
 */
public class TableSchemaTest extends BaseTest {

  @Test
  public void testColumnDefCreation() {
    ColumnDef col = new ColumnDef("name", "cf1", "name", AccumuloColumnType.VARCHAR, true);

    assertEquals("name", col.getName());
    assertEquals("cf1", col.getColumnFamily());
    assertEquals("name", col.getColumnQualifier());
    assertEquals(AccumuloColumnType.VARCHAR, col.getType());
    assertTrue(col.isNullable());
    assertEquals(SqlTypeName.VARCHAR, col.getSqlTypeName());
    assertEquals("cf1:name", col.getFullColumnName());
  }

  @Test
  public void testColumnDefFactoryMethods() {
    ColumnDef col1 = ColumnDef.create("age", "cf1", "age", AccumuloColumnType.INT);
    assertEquals("age", col1.getName());
    assertEquals(AccumuloColumnType.INT, col1.getType());
    assertTrue(col1.isNullable());

    ColumnDef col2 = ColumnDef.varchar("email", "cf2", "email");
    assertEquals("email", col2.getName());
    assertEquals(AccumuloColumnType.VARCHAR, col2.getType());
  }

  @Test
  public void testColumnDefFullColumnName() {
    ColumnDef col1 = ColumnDef.varchar("name", "cf1", "name");
    assertEquals("cf1:name", col1.getFullColumnName());

    ColumnDef col2 = ColumnDef.varchar("data", "cf1", "");
    assertEquals("cf1", col2.getFullColumnName());

    ColumnDef col3 = ColumnDef.varchar("data", "cf1", null);
    assertEquals("cf1", col3.getFullColumnName());
  }

  @Test
  public void testTableSchemaBuilder() {
    TableSchema schema = TableSchema.builder("users")
        .rowKeyType(AccumuloColumnType.VARCHAR)
        .addColumn("name", "cf1", "name", AccumuloColumnType.VARCHAR)
        .addColumn("age", "cf1", "age", AccumuloColumnType.INT)
        .addVarcharColumn("email", "cf2", "email")
        .build();

    assertEquals("users", schema.getTableName());
    assertEquals(AccumuloColumnType.VARCHAR, schema.getRowKeyType());
    assertEquals(3, schema.getColumnCount());
    assertTrue(schema.hasExplicitColumns());
  }

  @Test
  public void testTableSchemaColumnLookup() {
    TableSchema schema = TableSchema.builder("test")
        .addColumn("name", "cf1", "name", AccumuloColumnType.VARCHAR)
        .addColumn("age", "cf1", "age", AccumuloColumnType.INT)
        .build();

    // By name
    ColumnDef byName = schema.getColumnByName("name");
    assertNotNull(byName);
    assertEquals("name", byName.getName());

    ColumnDef byNameCase = schema.getColumnByName("NAME");
    assertNotNull(byNameCase);
    assertEquals("name", byNameCase.getName());

    ColumnDef notFound = schema.getColumnByName("notexist");
    assertNull(notFound);

    // By Accumulo key
    ColumnDef byKey = schema.getColumnByAccumuloKey("cf1:name");
    assertNotNull(byKey);
    assertEquals("name", byKey.getName());

    ColumnDef byKeyNotFound = schema.getColumnByAccumuloKey("cf2:name");
    assertNull(byKeyNotFound);
  }

  @Test
  public void testDynamicSchema() {
    TableSchema schema = TableSchema.dynamic("dynamic_table");

    assertEquals("dynamic_table", schema.getTableName());
    assertEquals(AccumuloColumnType.VARBINARY, schema.getRowKeyType());
    assertFalse(schema.hasExplicitColumns());
    assertEquals(0, schema.getColumnCount());
  }

  @Test
  public void testTableSchemaEquality() {
    TableSchema schema1 = TableSchema.builder("test")
        .addVarcharColumn("name", "cf1", "name")
        .build();

    TableSchema schema2 = TableSchema.builder("test")
        .addVarcharColumn("name", "cf1", "name")
        .build();

    TableSchema schema3 = TableSchema.builder("test")
        .addColumn("name", "cf1", "name", AccumuloColumnType.INT)
        .build();

    assertEquals(schema1, schema2);
    assertEquals(schema1.hashCode(), schema2.hashCode());
    assertFalse(schema1.equals(schema3));
  }

  @Test
  public void testTableSchemaJsonSerialization() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    TableSchema schema = TableSchema.builder("users")
        .rowKeyType(AccumuloColumnType.VARCHAR)
        .addColumn("name", "cf1", "name", AccumuloColumnType.VARCHAR)
        .addColumn("age", "cf1", "age", AccumuloColumnType.INT)
        .build();

    String json = mapper.writeValueAsString(schema);
    assertNotNull(json);
    assertTrue(json.contains("users"));
    assertTrue(json.contains("name"));
    assertTrue(json.contains("age"));

    TableSchema deserialized = mapper.readValue(json, TableSchema.class);
    assertEquals(schema.getTableName(), deserialized.getTableName());
    assertEquals(schema.getRowKeyType(), deserialized.getRowKeyType());
    assertEquals(schema.getColumnCount(), deserialized.getColumnCount());
  }

  @Test
  public void testColumnDefJsonSerialization() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    ColumnDef col = new ColumnDef("name", "cf1", "qualifier1", AccumuloColumnType.VARCHAR, false);

    String json = mapper.writeValueAsString(col);
    assertNotNull(json);
    assertTrue(json.contains("name"));
    assertTrue(json.contains("cf1"));
    assertTrue(json.contains("qualifier1"));
    assertTrue(json.contains("VARCHAR"));

    ColumnDef deserialized = mapper.readValue(json, ColumnDef.class);
    assertEquals(col, deserialized);
  }

  @Test
  public void testTableSchemaDefaultValues() {
    // Null rowKeyType should default to VARBINARY
    TableSchema schema = new TableSchema("test", null, null);
    assertEquals(AccumuloColumnType.VARBINARY, schema.getRowKeyType());
    assertFalse(schema.hasExplicitColumns());
  }
}
