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
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
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

public class ElasticSearchPlanTest extends ClusterTest {

  private static ElasticsearchClient elasticsearchClient;
  private static final List<String> indexNames = new LinkedList<>();

  @BeforeClass
  public static void init() throws Exception {
    TestElasticsearchSuite.initElasticsearch();
    startCluster(ClusterFixture.builder(dirTestWatcher));

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
    indexNames.add("nation");
    CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
        .index("nation")
        .build();
    elasticsearchClient.indices().create(createIndexRequest);

    final Reader input1 = new StringReader(
      JSONObject.toJSONString(ImmutableMap.of(
        "n_nationkey", 0,
        "n_name", "ALGERIA",
        "n_regionkey", 1
      ))
    );
    IndexRequest<JsonData> request = IndexRequest.of(i -> i
        .index("nation")
        .withJson(input1)
    );
    elasticsearchClient.index(request);

    RefreshRequest refreshRequest = new RefreshRequest.Builder()
        .index(indexNames)
        .build();

    elasticsearchClient.indices().refresh(refreshRequest);
  }

  @Test
  public void testProjectPushDown() throws Exception {
    queryBuilder()
        .sql("select n_name, n_nationkey from elastic.`nation`")
        .planMatcher()
        .include("ElasticsearchProject.*n_name.*n_nationkey")
        .exclude("\\*\\*")
        .match();
  }

  @Test
  public void testFilterPushDown() throws Exception {
    queryBuilder()
        .sql("select n_name, n_nationkey from elastic.`nation` where n_nationkey = 0")
        .planMatcher()
        .include("ElasticsearchFilter")
        .match();
  }

  @Test
  public void testFilterPushDownWithJoin() throws Exception {
    String query = "select * from elastic.`nation` e\n" +
        "join elastic.`nation` s on e.n_nationkey = s.n_nationkey where e.n_name = 'algeria'";

    queryBuilder()
        .sql(query)
        .planMatcher()
        .include("ElasticsearchFilter")
        .match();
  }

  @Test
  public void testAggregationPushDown() throws Exception {
    queryBuilder()
        .sql("select count(*) from elastic.`nation`")
        .planMatcher()
        .include("ElasticsearchAggregate.*COUNT")
        .match();
  }

  @Test
  public void testLimitWithSortPushDown() throws Exception {
    queryBuilder()
        .sql("select n_nationkey from elastic.`nation` order by n_name limit 3")
        .planMatcher()
        .include("ElasticsearchSort.*sort.*fetch")
        .match();
  }

  @Test
  public void testAggregationWithGroupByPushDown() throws Exception {
    queryBuilder()
        .sql("select sum(n_nationkey) from elastic.`nation` group by n_regionkey")
        .planMatcher()
        .include("ElasticsearchAggregate.*SUM")
        .match();
  }
}
