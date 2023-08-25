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

import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import com.google.common.collect.ImmutableMap;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.json.simple.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.JsonData;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ElasticInfoSchemaTest extends ClusterTest {

  private static final List<String> indexNames = new LinkedList<>();
  private static ElasticsearchClient elasticsearchClient;

  @BeforeClass
  public static void init() throws Exception {
    TestElasticsearchSuite.initElasticsearch();
    startCluster(ClusterFixture.builder(dirTestWatcher));

    ElasticsearchStorageConfig config = new ElasticsearchStorageConfig(
        Collections.singletonList(TestElasticsearchSuite.getAddress()),
        TestElasticsearchSuite.ELASTICSEARCH_USERNAME,
        TestElasticsearchSuite.ELASTICSEARCH_PASSWORD,
        null, AuthMode.SHARED_USER.name(), false,
        null
    );

    config.setEnabled(true);
    cluster.defineStoragePlugin("elastic", config);

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
    {
      indexNames.add("t1");
      CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
          .index("t1")
          .build();
      elasticsearchClient.indices().create(createIndexRequest);

      final Reader input1 = new StringReader(
        JSONObject.toJSONString(
          ImmutableMap.of("string_field", "a", "int_field", 123)
        )
      );
      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("t1")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      indexNames.add("t2");
      CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
          .index("t2")
          .build();
      elasticsearchClient.indices().create(createIndexRequest);

      final Reader input2 = new StringReader(
        JSONObject.toJSONString(
          ImmutableMap.of("another_string_field", "b", "another_int_field", 321)
        )
      );
      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("t2")
          .withJson(input2)
      );
      elasticsearchClient.index(request);

      RefreshRequest refreshRequest = new RefreshRequest.Builder()
          .index(indexNames)
          .build();
      elasticsearchClient.indices().refresh(refreshRequest);
    }
  }

  @Test
  public void testShowTables() throws Exception {
    testBuilder()
        .sqlQuery("show tables in elastic")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("elastic", "t1")
        .baselineValues("elastic", "t2")
        .go();
  }

  @Test
  public void testShowTablesLike() throws Exception {
    testBuilder()
        .sqlQuery("show tables in elastic like '%2%'")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("elastic", "t2")
        .go();
  }
}
