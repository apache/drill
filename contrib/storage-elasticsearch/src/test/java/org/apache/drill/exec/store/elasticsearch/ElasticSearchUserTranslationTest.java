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

package org.apache.drill.exec.store.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.JsonData;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.hamcrest.CoreMatchers;
import org.json.simple.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class ElasticSearchUserTranslationTest extends ClusterTest {
  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUserTranslationTest.class);
  private static final List<String> indexNames = new LinkedList<>();
  private static ElasticsearchClient elasticsearchClient;

  @BeforeClass
  public static void init() throws Exception {
    TestElasticsearchSuite.initElasticsearch();
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
        .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);

    startCluster(builder);


    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(new HashMap<>());
    // Add authorized user
    credentialsProvider.setUserCredentials(TestElasticsearchSuite.ELASTICSEARCH_USERNAME,
        TestElasticsearchSuite.ELASTICSEARCH_PASSWORD, TEST_USER_1);
    // Add unauthorized user
    credentialsProvider.setUserCredentials("nope", "no way dude", TEST_USER_2);

    ElasticsearchStorageConfig config = new ElasticsearchStorageConfig(
        Collections.singletonList(TestElasticsearchSuite.getAddress()),
        TestElasticsearchSuite.ELASTICSEARCH_USERNAME,
        TestElasticsearchSuite.ELASTICSEARCH_PASSWORD,
        null,
        AuthMode.SHARED_USER.name(),
        null
    );

    config.setEnabled(true);
    cluster.defineStoragePlugin("elastic", config);

    ElasticsearchStorageConfig ut_config = new ElasticsearchStorageConfig(
        Collections.singletonList(TestElasticsearchSuite.getAddress()),
        null,
        null,
        null,
        AuthMode.USER_TRANSLATION.name(),
        credentialsProvider);

    ut_config.setEnabled(true);
    cluster.defineStoragePlugin("ut_elastic", ut_config);

    elasticsearchClient = TestElasticsearchSuite.getESClient();
    prepareData();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder()
        .index(indexNames)
        .build();
    elasticsearchClient.indices().delete(deleteIndexRequest);
    TestElasticsearchSuite.tearDownCluster();
  }

  private static void prepareData() throws IOException {
    String indexName = "t1";
    indexNames.add(indexName);
    CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
        .index(indexName)
        .build();

    elasticsearchClient.indices().create(createIndexRequest);

    Map<String, Object> map = new HashMap<>();
    map.put("string_field", "a");
    map.put("int_field", 123);
    JSONObject jsonObject = new JSONObject(map);

    Reader input = new StringReader(jsonObject.toJSONString());
    IndexRequest<JsonData> request = IndexRequest.of(i -> i
        .index("t1")
        .withJson(input)
    );

    IndexResponse response = elasticsearchClient.index(request);
    logger.debug("Insert response: {}", response.toString() );

    RefreshRequest refreshRequest = new RefreshRequest.Builder()
        .index(indexNames)
        .build();
    elasticsearchClient.indices().refresh(refreshRequest);

    indexName = "t2";
    indexNames.add(indexName);
    createIndexRequest = new CreateIndexRequest.Builder()
        .index(indexName)
        .build();

    elasticsearchClient.indices().create(createIndexRequest);

    map = new HashMap<>();
    map.put("another_int_field", 321);
    map.put("another_string_field", "b");
    jsonObject = new JSONObject(map);

    Reader input2 = new StringReader(jsonObject.toJSONString());
    request = IndexRequest.of(i -> i
        .index("t2")
        .withJson(input2)
    );

    response = elasticsearchClient.index(request);
    logger.debug("Insert response: {}", response.toString() );

    refreshRequest = new RefreshRequest.Builder()
        .index(indexNames)
        .build();
    elasticsearchClient.indices().refresh(refreshRequest);
    logger.debug("Data preparation complete.");
  }

  @Test
  public void testInfoSchemaQueryWithMissingCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to ElasticSearch.
    // This user should not see the ut_elastic because they do not have valid credentials.
    ClientFixture client = cluster
        .clientBuilder()
        .property(DrillProperties.USER, ADMIN_USER)
        .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
        .build();

    String sql = "SHOW DATABASES WHERE schema_name LIKE '%elastic%'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();
    assertEquals(1, results.rowCount());
    results.clear();
  }

  @Test
  public void testInfoSchemaQueryWithValidCredentials() throws Exception {
    // This test validates that the ElasticSearch connection with user translation appears when the user is
    // authenticated.
    ClientFixture client = cluster
        .clientBuilder()
        .property(DrillProperties.USER, TEST_USER_1)
        .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
        .build();

    String sql = "SHOW DATABASES WHERE schema_name LIKE '%elastic%'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(2, results.rowCount());
    results.clear();
  }

  @Test
  public void testQueryWithUserTranslation() throws Exception {
    ClientFixture client = cluster
        .clientBuilder()
        .property(DrillProperties.USER, TEST_USER_1)
        .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
        .build();

    String sql = "select * from ut_elastic.t1";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
    results.clear();
  }

  @Test
  public void testQueryWithUserTranslationAndInvalidCredentials() throws Exception {
    ClientFixture client = cluster
        .clientBuilder()
        .property(DrillProperties.USER, ADMIN_USER)
        .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
        .build();

    String sql = "select * from ut_elastic.t1";
    try {
      client.queryBuilder().sql(sql).rowSet();
      fail();
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString("Object 'ut_elastic' not found"));
    }
  }
}
