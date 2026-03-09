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
package org.apache.drill.exec.dotdrill;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for MaterializedView data model serialization and deserialization.
 */
@Category(SqlTest.class)
public class TestMaterializedView extends BaseTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testJsonSerialization() throws Exception {
    List<View.Field> fields = Arrays.asList(
        new View.Field("id", SqlTypeName.BIGINT, null, null, null, null, null, true, null, null),
        new View.Field("name", SqlTypeName.VARCHAR, 100, null, null, null, null, true, null, null)
    );
    List<String> schemaPath = Arrays.asList("dfs", "tmp");

    MaterializedView mv = new MaterializedView(
        "test_mv",
        "SELECT id, name FROM t1",
        fields,
        schemaPath,
        "test_mv",
        1234567890L,
        MaterializedView.RefreshStatus.COMPLETE
    );

    // Serialize to JSON
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, mv);
    String json = writer.toString();

    assertNotNull(json);
    assertTrue(json.contains("test_mv"));
    assertTrue(json.contains("SELECT id, name FROM t1"));
    assertTrue(json.contains("COMPLETE"));

    // Deserialize from JSON
    MaterializedView deserialized = mapper.readValue(new StringReader(json), MaterializedView.class);

    assertEquals("test_mv", deserialized.getName());
    assertEquals("SELECT id, name FROM t1", deserialized.getSql());
    assertEquals(2, deserialized.getFields().size());
    assertEquals("dfs", deserialized.getWorkspaceSchemaPath().get(0));
    assertEquals("tmp", deserialized.getWorkspaceSchemaPath().get(1));
    assertEquals("test_mv", deserialized.getDataStoragePath());
    assertEquals(Long.valueOf(1234567890L), deserialized.getLastRefreshTime());
    assertEquals(MaterializedView.RefreshStatus.COMPLETE, deserialized.getRefreshStatus());
  }

  @Test
  public void testJsonDeserializationWithMinimalFields() throws Exception {
    String json = "{\"name\":\"mv1\",\"sql\":\"SELECT * FROM t\",\"fields\":[],\"workspaceSchemaPath\":[\"dfs\"]}";

    MaterializedView mv = mapper.readValue(new StringReader(json), MaterializedView.class);

    assertEquals("mv1", mv.getName());
    assertEquals("SELECT * FROM t", mv.getSql());
    assertEquals("dfs", mv.getWorkspaceSchemaPath().get(0));
    // Default values
    assertEquals("mv1", mv.getDataStoragePath()); // Defaults to name
    assertEquals(MaterializedView.RefreshStatus.INCOMPLETE, mv.getRefreshStatus());
  }

  @Test
  public void testMarkRefreshed() {
    List<View.Field> fields = Collections.emptyList();
    List<String> schemaPath = Arrays.asList("dfs", "tmp");

    MaterializedView mv = new MaterializedView(
        "test_mv",
        "SELECT * FROM t1",
        fields,
        schemaPath,
        "test_mv",
        null,
        MaterializedView.RefreshStatus.INCOMPLETE
    );

    assertEquals(MaterializedView.RefreshStatus.INCOMPLETE, mv.getRefreshStatus());

    long beforeRefresh = System.currentTimeMillis();
    mv.markRefreshed();
    long afterRefresh = System.currentTimeMillis();

    assertEquals(MaterializedView.RefreshStatus.COMPLETE, mv.getRefreshStatus());
    assertTrue(mv.getLastRefreshTime() >= beforeRefresh);
    assertTrue(mv.getLastRefreshTime() <= afterRefresh);
  }

  @Test
  public void testWithRefreshInfo() {
    List<View.Field> fields = Collections.emptyList();
    List<String> schemaPath = Arrays.asList("dfs", "tmp");

    MaterializedView mv = new MaterializedView(
        "test_mv",
        "SELECT * FROM t1",
        fields,
        schemaPath,
        "test_mv",
        1000L,
        MaterializedView.RefreshStatus.INCOMPLETE
    );

    MaterializedView updated = mv.withRefreshInfo(2000L, MaterializedView.RefreshStatus.COMPLETE);

    // Original should be unchanged
    assertEquals(Long.valueOf(1000L), mv.getLastRefreshTime());
    assertEquals(MaterializedView.RefreshStatus.INCOMPLETE, mv.getRefreshStatus());

    // Updated should have new values
    assertEquals(Long.valueOf(2000L), updated.getLastRefreshTime());
    assertEquals(MaterializedView.RefreshStatus.COMPLETE, updated.getRefreshStatus());

    // Other fields should be the same
    assertEquals(mv.getName(), updated.getName());
    assertEquals(mv.getSql(), updated.getSql());
  }

  @Test
  public void testSchemaPathLowerCase() {
    List<View.Field> fields = Collections.emptyList();
    List<String> schemaPath = Arrays.asList("DFS", "TMP");

    MaterializedView mv = new MaterializedView(
        "test_mv",
        "SELECT * FROM t1",
        fields,
        schemaPath,
        "test_mv",
        null,
        null
    );

    // Schema path should be converted to lower case
    assertEquals("dfs", mv.getWorkspaceSchemaPath().get(0));
    assertEquals("tmp", mv.getWorkspaceSchemaPath().get(1));
  }

  @Test
  public void testIsDynamic() {
    // With no fields - dynamic
    MaterializedView mvDynamic = new MaterializedView(
        "mv1",
        "SELECT * FROM t1",
        Collections.emptyList(),
        Collections.singletonList("dfs"),
        "mv1",
        null,
        null
    );
    assertTrue(mvDynamic.isDynamic());

    // With fields - not dynamic
    List<View.Field> fields = Arrays.asList(
        new View.Field("id", SqlTypeName.BIGINT, null, null, null, null, null, true, null, null)
    );
    MaterializedView mvStatic = new MaterializedView(
        "mv2",
        "SELECT id FROM t1",
        fields,
        Collections.singletonList("dfs"),
        "mv2",
        null,
        null
    );
    assertEquals(false, mvStatic.isDynamic());
  }

  @Test
  public void testFieldTypes() throws Exception {
    // Test various field types can be serialized/deserialized
    List<View.Field> fields = Arrays.asList(
        new View.Field("col_bigint", SqlTypeName.BIGINT, null, null, null, null, null, true, null, null),
        new View.Field("col_varchar", SqlTypeName.VARCHAR, 255, null, null, null, null, true, null, null),
        new View.Field("col_decimal", SqlTypeName.DECIMAL, 10, 2, null, null, null, true, null, null),
        new View.Field("col_boolean", SqlTypeName.BOOLEAN, null, null, null, null, null, false, null, null),
        new View.Field("col_double", SqlTypeName.DOUBLE, null, null, null, null, null, true, null, null),
        new View.Field("col_timestamp", SqlTypeName.TIMESTAMP, null, null, null, null, null, true, null, null)
    );

    MaterializedView mv = new MaterializedView(
        "mv_types",
        "SELECT * FROM t",
        fields,
        Collections.singletonList("dfs"),
        "mv_types",
        null,
        null
    );

    // Serialize and deserialize
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, mv);
    MaterializedView deserialized = mapper.readValue(new StringReader(writer.toString()), MaterializedView.class);

    assertEquals(6, deserialized.getFields().size());
    assertEquals(SqlTypeName.BIGINT, deserialized.getFields().get(0).getType());
    assertEquals(SqlTypeName.VARCHAR, deserialized.getFields().get(1).getType());
    assertEquals(Integer.valueOf(255), deserialized.getFields().get(1).getPrecision());
    assertEquals(SqlTypeName.DECIMAL, deserialized.getFields().get(2).getType());
    assertEquals(Integer.valueOf(10), deserialized.getFields().get(2).getPrecision());
    assertEquals(Integer.valueOf(2), deserialized.getFields().get(2).getScale());
    assertEquals(SqlTypeName.BOOLEAN, deserialized.getFields().get(3).getType());
    assertEquals(false, deserialized.getFields().get(3).getIsNullable());
  }
}
