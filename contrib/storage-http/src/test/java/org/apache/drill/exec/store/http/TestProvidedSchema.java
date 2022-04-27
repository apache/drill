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
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;

import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.http.providedSchema.HttpField;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
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


public class TestProvidedSchema extends ClusterTest {
  private static final int MOCK_SERVER_PORT = 47777;

  private static String TEST_JSON_PAGE1;
  private static String TEST_SCHEMA_CHANGE1;
  private static String TEST_JSON_PAGE2;
  private static String TEST_JSON_PAGE3;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    TEST_JSON_PAGE1 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/p1.json"), Charsets.UTF_8).read();
    TEST_SCHEMA_CHANGE1 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/schema_change_1.json"), Charsets.UTF_8).read();
    TEST_JSON_PAGE2 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/p2.json"), Charsets.UTF_8).read();
    TEST_JSON_PAGE3 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/p3.json"), Charsets.UTF_8).read();

    dirTestWatcher.copyResourceToRoot(Paths.get("data/"));
    makeMockConfig(cluster);
  }

  public static void makeMockConfig(ClusterFixture cluster) {

    List<HttpField> schema = new ArrayList<>();
    schema.add(new HttpField("col_1", "DOUBLE"));
    schema.add(new HttpField("col_2", "DOUBLE"));
    schema.add(new HttpField("col_3", "DOUBLE"));

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
    mapSchema.add(new HttpField("field1", "VARCHAR"));
    List<HttpField> innerMap = new ArrayList<>();
    innerMap.add(new HttpField("nested_value1", "varchar"));
    innerMap.add(new HttpField("nested_value2", "varchar"));
    mapSchema.add(new HttpField("field2", "MAP", innerMap));

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


    List<HttpField> nestedFields = new ArrayList<>();
    nestedFields.add(new HttpField("nested_value1", "varchar"));
    nestedFields.add(new HttpField("nested_value2", "varchar"));

    List<HttpField> innerMapSchema = new ArrayList<>();
    innerMapSchema.add(new HttpField("field2", "map", nestedFields));

    List<HttpField> unionSchema = new ArrayList<>();
    unionSchema.add(new HttpField("field1", "varchar"));
    unionSchema.add(new HttpField("field2", "union", innerMapSchema));

    HttpJsonOptions unionJsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(unionSchema)
      .skipMalformedRecords(true)
      .build();

    HttpApiConfig unionApi = HttpApiConfig.builder()
      .url("http://localhost:47777/json")
      .method("get")
      .jsonOptions(unionJsonOptions)
      .requireTail(false)
      .inputType("json")
      .build();


    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("basicJson", basicJson);
    configs.put("schemaChange", schemaChange);
    configs.put("union", unionApi);


    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, 2, null, null, "", 80, "", "", "", null, PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
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
      results.print();

      /*TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1.0, 2.0, 3.0)
        .addRow(4.0, 5.0, 6.0)
        .build();

      RowSetUtilities.verify(expected, results);*/
    }
  }

  @Test
  public void testSchemaChangeWithUnion() throws Exception {
    String sql = "SELECT * FROM `local`.`union`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_SCHEMA_CHANGE1));
      RowSet results = client.queryBuilder().sql(sql).rowSet();
      results.print();

      /*TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1.0, 2.0, 3.0)
        .addRow(4.0, 5.0, 6.0)
        .build();

      RowSetUtilities.verify(expected, results);*/
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
