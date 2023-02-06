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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.JsonData;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.drill.test.TestBuilder.listOf;
import static org.apache.drill.test.TestBuilder.mapOf;

public class ElasticComplexTypesTest extends ClusterTest {

  private static final Logger logger = LoggerFactory.getLogger(ElasticComplexTypesTest.class);
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
        null, AuthMode.SHARED_USER.name(),
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
      indexNames.add("arr");
      CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
          .index("arr")
          .build();
      elasticsearchClient.indices().create(createIndexRequest);

      final Reader input1 = new StringReader(
        JSONObject.toJSONString(ImmutableMap.of(
          "string_arr", Arrays.asList("a", "b", "c", "d"),
          "int_arr", Arrays.asList(1, 2, 3, 4, 0),
          "nest_int_arr", Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 0))
        ))
      );
      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("arr")
          .withJson(input1)
      );
      logger.debug("Insert response {}", elasticsearchClient.index(request));
    }
    {
      indexNames.add("map");
      CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
          .index("map")
          .build();
      elasticsearchClient.indices().create(createIndexRequest);

      Map<String, Object> map = ImmutableMap.of("a", 123, "b", "abc");
      Map<String, Object> nested_map = ImmutableMap.of(
        "a", 123,
        "b", ImmutableMap.of("c", "abc")
      );
      final Reader input1 = new StringReader(
        JSONObject.toJSONString(ImmutableMap.of(
          "prim_field", 321,
          "nest_field", map,
          "more_nest_field", nested_map,
          "map_arr", Collections.singletonList(nested_map)
        ))
      );
      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("map")
          .withJson(input1)
      );
      logger.debug("Insert response {}", elasticsearchClient.index(request));

      RefreshRequest refreshRequest = new RefreshRequest.Builder()
          .index(indexNames)
          .build();
      elasticsearchClient.indices().refresh(refreshRequest);
      logger.debug("Data preparation complete.");
    }
  }

  @Test
  public void testSelectStarWithArray() throws Exception {
    testBuilder()
        .sqlQuery("select * from elastic.arr")
        .unOrdered()
        .baselineColumns("string_arr", "int_arr", "nest_int_arr")
        .baselineValues(listOf("a", "b", "c", "d"), listOf(1, 2, 3, 4, 0),
            listOf(listOf(1, 2), listOf(3, 4, 0)))
        .go();
  }

  @Test
  public void testSelectArrayElem() throws Exception {
    testBuilder()
        .sqlQuery("select string_arr[0] c1, int_arr[1] c2, nest_int_arr[0][1] c3 from elastic.arr")
        .unOrdered()
        .baselineColumns("c1", "c2", "c3")
        .baselineValues("a", 2, 2)
        .go();
  }

  @Test
  public void testSelectStarWithJson() throws Exception {
    testBuilder()
        .sqlQuery("select * from elastic.map")
        .unOrdered()
        .baselineColumns("prim_field", "nest_field", "more_nest_field", "map_arr")
        .baselineValues(321, mapOf("a", 123, "b", "abc"),
            mapOf("a", 123, "b", mapOf("c", "abc")),
            listOf(mapOf("a", 123, "b", mapOf("c", "abc"))))
        .go();
  }

  @Test
  public void testSelectNestedFields() throws Exception {
    testBuilder()
        .sqlQuery("select m.nest_field.a a, m.nest_field.b b, m.more_nest_field.b.c c, map_arr[0].b.c d from elastic.map m")
        .unOrdered()
        .baselineColumns("a", "b", "c", "d")
        .baselineValues(123, "abc", "abc", "abc")
        .go();
  }
}
