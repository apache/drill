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
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.alias.AliasRegistry;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_GROUP;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;
import static org.junit.Assert.assertEquals;

public class TestHttpUDFWithAliases extends ClusterTest {

  private static AliasRegistry storageAliasesRegistry;
  private static AliasRegistry tableAliasesRegistry;
  private static final int MOCK_SERVER_PORT = 47778;
  private static String TEST_JSON_PAGE1;
  private static final String DUMMY_URL = "http://localhost:" + MOCK_SERVER_PORT;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    TEST_JSON_PAGE1 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/p1.json"), Charsets.UTF_8).read();

    cluster = ClusterFixture.bareBuilder(dirTestWatcher)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
      .configProperty(DrillProperties.USER, PROCESS_USER)
      .configProperty(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD)
      .build();

    ClientFixture admin = cluster.clientBuilder()
      .property(DrillProperties.USER, PROCESS_USER)
      .property(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD)
      .build();

    admin.alterSystem(ExecConstants.ADMIN_USERS_KEY, ADMIN_USER + "," + PROCESS_USER);
    admin.alterSystem(ExecConstants.ADMIN_USER_GROUPS_KEY, ADMIN_GROUP);

    client = cluster.clientBuilder()
      .property(DrillProperties.USER, ADMIN_USER)
      .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
      .build();
    storageAliasesRegistry = cluster.drillbit().getContext().getAliasRegistryProvider().getStorageAliasesRegistry();
    tableAliasesRegistry = cluster.drillbit().getContext().getAliasRegistryProvider().getTableAliasesRegistry();

    TupleMetadata simpleSchema = new SchemaBuilder()
      .addNullable("col_1", MinorType.FLOAT8)
      .addNullable("col_2", MinorType.FLOAT8)
      .addNullable("col_3", MinorType.FLOAT8)
      .build();

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .schema(simpleSchema)
      .build();

    HttpApiConfig basicJson = HttpApiConfig.builder()
      .url(String.format("%s/json", DUMMY_URL))
      .method("get")
      .jsonOptions(jsonOptions)
      .requireTail(false)
      .inputType("json")
      .build();

    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("basicJson", basicJson);

    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, 200, "globaluser", "globalpass", "",
        80, "", "", "", null, new PlainCredentialsProvider(ImmutableMap.of(
        UsernamePasswordCredentials.USERNAME, "globaluser",
        UsernamePasswordCredentials.PASSWORD, "globalpass")), AuthMode.SHARED_USER.name());
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("local", mockStorageConfigWithWorkspace);
  }

  @Test
  public void testSeveralRowsAndRequestsAndPublicStorageAlias() throws Exception {
    storageAliasesRegistry.createPublicAliases();
    storageAliasesRegistry.getPublicAliases().put("`foobar`", "`local`", false);

    String sql = "SELECT http_request('foobar.basicJson', `col1`) as data FROM cp.`/data/p4.json`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));

      RowSet results = client.queryBuilder().sql(sql).rowSet();

      assertEquals(2, results.rowCount());

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("data")
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(singleMap(mapValue(1.0, 2.0, 3.0)))
        .addRow(singleMap(mapValue(4.0, 5.0, 6.0)))
        .build();

      RowSetUtilities.verify(expected, results);
    } finally {
      storageAliasesRegistry.deletePublicAliases();
    }
  }

  @Test
  public void testSeveralRowsAndRequestsAndUserStorageAlias() throws Exception {
    String sql = "SELECT http_request('foobar.basicJson', `col1`) as data FROM cp.`/data/p4.json`";
    try (MockWebServer server = startServer()) {

      ClientFixture client = cluster.clientBuilder()
        .property(DrillProperties.USER, TEST_USER_2)
        .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
        .build();

      storageAliasesRegistry.createUserAliases(TEST_USER_2);
      storageAliasesRegistry.getUserAliases(TEST_USER_2).put("`foobar`", "`local`", false);

      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));

      RowSet results = client.queryBuilder().sql(sql).rowSet();

      assertEquals(2, results.rowCount());

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("data")
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(singleMap(mapValue(1.0, 2.0, 3.0)))
        .addRow(singleMap(mapValue(4.0, 5.0, 6.0)))
        .build();

      RowSetUtilities.verify(expected, results);
    } finally {
      storageAliasesRegistry.deleteUserAliases(TEST_USER_2);
    }
  }

  @Test
  public void testSeveralRowsAndRequestsAndPublicTableAlias() throws Exception {
    tableAliasesRegistry.createPublicAliases();
    tableAliasesRegistry.getPublicAliases().put("`foobar`", "`basicJson`", false);

    String sql = "SELECT http_request('local.foobar', `col1`) as data FROM cp.`/data/p4.json`";
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));

      RowSet results = client.queryBuilder().sql(sql).rowSet();

      assertEquals(2, results.rowCount());

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("data")
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(singleMap(mapValue(1.0, 2.0, 3.0)))
        .addRow(singleMap(mapValue(4.0, 5.0, 6.0)))
        .build();

      RowSetUtilities.verify(expected, results);
    } finally {
      tableAliasesRegistry.deletePublicAliases();
    }
  }

  @Test
  public void testSeveralRowsAndRequestsAndUserTableAlias() throws Exception {
    String sql = "SELECT http_request('local.foobar', `col1`) as data FROM cp.`/data/p4.json`";
    try (MockWebServer server = startServer()) {

      ClientFixture client = cluster.clientBuilder()
        .property(DrillProperties.USER, TEST_USER_2)
        .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
        .build();

      tableAliasesRegistry.createUserAliases(TEST_USER_2);
      tableAliasesRegistry.getUserAliases(TEST_USER_2).put("`foobar`", "`basicJson`", false);

      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_PAGE1));

      RowSet results = client.queryBuilder().sql(sql).rowSet();

      assertEquals(2, results.rowCount());

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("data")
        .addNullable("col_1", MinorType.FLOAT8)
        .addNullable("col_2", MinorType.FLOAT8)
        .addNullable("col_3", MinorType.FLOAT8)
        .resumeSchema()
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(singleMap(mapValue(1.0, 2.0, 3.0)))
        .addRow(singleMap(mapValue(4.0, 5.0, 6.0)))
        .build();

      RowSetUtilities.verify(expected, results);
    } finally {
      tableAliasesRegistry.deleteUserAliases(TEST_USER_2);
    }
  }

  public static MockWebServer startServer() throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}
