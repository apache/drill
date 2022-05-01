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

package org.apache.drill.exec.store.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;

import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.http.providedSchema.HttpField;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;


public class TestProvidedSchema extends ClusterTest {
  private static final int MOCK_SERVER_PORT = 47777;

  private static String TEST_JSON_PAGE1;
  private static String TEST_SCHEMA_CHANGE1;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    TEST_JSON_PAGE1 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/p1.json"), Charsets.UTF_8).read();
    TEST_SCHEMA_CHANGE1 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/schema_change_1.json"), Charsets.UTF_8).read();

    dirTestWatcher.copyResourceToRoot(Paths.get("data/"));
    makeMockConfig(cluster);
  }

  public static void makeMockConfig(ClusterFixture cluster) {

    List<HttpField> schema = new ArrayList<>();
    schema.add(HttpField.builder().fieldName("col_1").fieldType("DOUBLE").build());
    schema.add(HttpField.builder().fieldName("col_2").fieldType("DOUBLE").build());
    schema.add(HttpField.builder().fieldName("col_3").fieldType("DOUBLE").build());

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(schema)
      .build();

    HttpApiConfig basicJson = HttpApiConfig.builder()
      .url("http://localhost:47777/json")
      .method("get")
      .jsonOptions(jsonOptions)
      .requireTail(false)
      .inputType("json")
      .build();

    List<HttpField> mapSchema = new ArrayList<>();
    mapSchema.add(HttpField.builder().fieldName("field1").fieldType("VARCHAR").build());
    List<HttpField> innerMap = new ArrayList<>();
    innerMap.add(HttpField.builder().fieldName("nested_value1").fieldType("varchar").build());
    innerMap.add(HttpField.builder().fieldName("nested_value2").fieldType("varchar").build());
    mapSchema.add(HttpField.builder().fieldName("field2").fieldType("MAP").fields(innerMap).build());

    HttpJsonOptions jsonOptionsSchemaChange = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(mapSchema)
      .skipMalformedRecords(true)
      .build();

    HttpApiConfig schemaChange = HttpApiConfig.builder()
      .url("http://localhost:47777/json")
      .method("get")
      .jsonOptions(jsonOptionsSchemaChange)
      .requireTail(false)
      .inputType("json")
      .build();

    List<HttpField> partialMapSchema = new ArrayList<>();
    partialMapSchema.add(HttpField.builder().fieldName("field1").fieldType("VARCHAR").build());
    List<HttpField> partialInnerMap = new ArrayList<>();
    partialInnerMap.add(HttpField.builder().fieldName("nested_value1").fieldType("varchar").build());
    partialMapSchema.add(HttpField.builder().fieldName("field2").fieldType("MAP").fields(partialInnerMap).build());


    HttpApiConfig partialSchema = HttpApiConfig.builder()
      .url("http://localhost:47777/json")
      .method("get")
      .jsonOptions(HttpJsonOptions.builder().providedSchema(partialMapSchema).build())
      .requireTail(false)
      .inputType("json")
      .build();

    Map<String,String> properties = new HashMap<>();
    properties.put("drill.json-mode", "json");
    List<HttpField> jsonModeSchema = new ArrayList<>();
    jsonModeSchema.add(HttpField.builder().fieldName("field1").fieldType("VARCHAR").build());
    jsonModeSchema.add(HttpField.builder().fieldName("field2").fieldType("VARCHAR").properties(properties).build());


    HttpJsonOptions jsonModeOptions = HttpJsonOptions.builder()
      .providedSchema(jsonModeSchema)
      .skipMalformedRecords(true)
      .build();

    HttpApiConfig jsonModeConfig = HttpApiConfig.builder()
      .url("http://localhost:47777/json")
      .method("get")
      .jsonOptions(jsonModeOptions)
      .requireTail(false)
      .inputType("json")
      .build();

    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("basicJson", basicJson);
    configs.put("schemaChange", schemaChange);
    configs.put("partialSchema", partialSchema);
    configs.put("jsonMode", jsonModeConfig);

    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, 2, "globaluser", "globalpass", "",
        80, "", "", "", null, new PlainCredentialsProvider(ImmutableMap.of(
        UsernamePasswordCredentials.USERNAME, "globaluser",
        UsernamePasswordCredentials.PASSWORD, "globalpass")), AuthMode.SHARED_USER.name());
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("local", mockStorageConfigWithWorkspace);
  }

  @Test
  public void testProvidedSchema() throws Exception {
    String sql = "SELECT * FROM `local`.`basicJson`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1.0, 2.0, 3.0)
        .addRow(4.0, 5.0, 6.0)
        .build();

      RowSetUtilities.verify(expected, results);
    }
  }

  @Test
  public void testSchemaChangeWithProvidedSchema() throws Exception {
    String sql = "SELECT * FROM `local`.`schemaChange`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_SCHEMA_CHANGE1));
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("field1", MinorType.VARCHAR)
        .addMap("field2")
          .addNullable("nested_value1", MinorType.VARCHAR)
          .addNullable("nested_value2", MinorType.VARCHAR)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("value1", strArray(null, null))
        .addRow("value3", strArray("nv1", "nv2"))
        .addRow("value5", strArray("nv3", "nv4"))
        .build();

      RowSetUtilities.verify(expected, results);
    }
  }

  @Test
  public void testPartialSchema() throws Exception {
    String sql = "SELECT * FROM `local`.`partialSchema`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_SCHEMA_CHANGE1));
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("field1", MinorType.VARCHAR)
        .addMap("field2")
        .addNullable("nested_value1", MinorType.VARCHAR)
        .addNullable("nested_value2", MinorType.VARCHAR)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("value1", strArray(null, null))
        .addRow("value3", strArray("nv1", "nv2"))
        .addRow("value5", strArray("nv3", "nv4"))
        .build();

      RowSetUtilities.verify(expected, results);
    }
  }

  @Test
  public void testPartialJSONSchema() throws Exception {
    String sql = "SELECT * FROM `local`.`partialSchema`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_SCHEMA_CHANGE1));
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("field1", MinorType.VARCHAR)
        .addMap("field2")
        .addNullable("nested_value1", MinorType.VARCHAR)
        .addNullable("nested_value2", MinorType.VARCHAR)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("value1", strArray(null, null))
        .addRow("value3", strArray("nv1", "nv2"))
        .addRow("value5", strArray("nv3", "nv4"))
        .build();

      RowSetUtilities.verify(expected, results);
    }
  }

  @Test
  public void testJsonMode() throws Exception {
    String sql = "SELECT * FROM `local`.`jsonMode`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_SCHEMA_CHANGE1));
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("field1", MinorType.VARCHAR)
        .addNullable("field2", MinorType.VARCHAR)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("value1", "value2")
        .addRow("value3", "{\"nested_value1\": nv1, \"nested_value2\": nv2}")
        .addRow("value5", "{\"nested_value1\": nv3, \"nested_value2\": nv4}")
        .build();

      RowSetUtilities.verify(expected, results);
    }
  }
  /**
   * Helper function to start the MockHTTPServer
   * @return Started Mock server
   * @throws IOException If the server cannot start, throws IOException
   */
  public MockWebServer startServer() throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}
