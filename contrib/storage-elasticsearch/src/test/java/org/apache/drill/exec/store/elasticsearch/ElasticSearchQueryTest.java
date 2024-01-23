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

import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ElasticSearchQueryTest extends ClusterTest {

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

  public static void prepareData() throws IOException {
    indexNames.add("employee");
    CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
        .index("employee")
        .build();
    elasticsearchClient.indices().create(createIndexRequest);

    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 1);
      inputMap.put("full_name", "Sheri Nowmer");
      inputMap.put("first_name", "Sheri");
      inputMap.put("last_name", "Nowmer");
      inputMap.put("position_id", 1);
      inputMap.put("position_title", "President");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 1);
      inputMap.put("birth_date", "1961-08-26");
      inputMap.put("hire_date", "1994-12-01 00:00:00.0");
      inputMap.put("salary", 80000.0);
      inputMap.put("supervisor_id", 0);
      inputMap.put("education_level", "Graduate Degree");
      inputMap.put("marital_status", "S");
      inputMap.put("gender", "F");
      inputMap.put("management_role", "Senior Management");
      inputMap.put("binary_field", Base64.getEncoder().encodeToString(
        "Senior Management".getBytes(StandardCharsets.UTF_8)
      ));
      inputMap.put("boolean_field", true);
      inputMap.put("date_field", "2015/01/01 12:10:30");
      inputMap.put("byte_field", (byte) 123);
      inputMap.put("long_field", 123L);
      inputMap.put("float_field", 123F);
      inputMap.put("short_field", (short) 123);
      inputMap.put("decimal_field", new BigDecimal("123.45"));

      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));
      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 2);
      inputMap.put("full_name", "Derrick Whelply");
      inputMap.put("first_name", "Derrick");
      inputMap.put("last_name", "Whelply");
      inputMap.put("position_id", 2);
      inputMap.put("position_title", "VP Country Manager");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 1);
      inputMap.put("birth_date", "1915-07-03");
      inputMap.put("hire_date", "1994-12-01 00:00:00.0");
      inputMap.put("salary", 40000.0);
      inputMap.put("supervisor_id", 1);
      inputMap.put("education_level", "Graduate Degree");
      inputMap.put("marital_status", "M");
      inputMap.put("gender", "M");
      inputMap.put("management_role", "Senior Management");

      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));
      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 4);
      inputMap.put("full_name", "Michael Spence");
      inputMap.put("first_name", "Michael");
      inputMap.put("last_name", "Spence");
      inputMap.put("position_id", 2);
      inputMap.put("position_title", "VP Country Manager");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 1);
      inputMap.put("birth_date", "1969-06-20");
      inputMap.put("hire_date", "1998-01-01 00:00:00.0");
      inputMap.put("salary", 40000.0);
      inputMap.put("supervisor_id", 1);
      inputMap.put("education_level", "Graduate Degree");
      inputMap.put("marital_status", "S");
      inputMap.put("gender", "M");
      inputMap.put("management_role", "Senior Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 5);
      inputMap.put("full_name", "Maya Gutierrez");
      inputMap.put("first_name", "Maya");
      inputMap.put("last_name", "Gutierrez");
      inputMap.put("position_id", 2);
      inputMap.put("position_title", "VP Country Manager");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 1);
      inputMap.put("birth_date", "1951-05-10");
      inputMap.put("hire_date", "1998-01-01 00:00:00.0");
      inputMap.put("salary", 35000.0);
      inputMap.put("supervisor_id", 1);
      inputMap.put("education_level", "Bachelors Degree");
      inputMap.put("marital_status", "M");
      inputMap.put("gender", "F");
      inputMap.put("management_role", "Senior Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 6);
      inputMap.put("full_name", "Roberta Damstra");
      inputMap.put("first_name", "Roberta");
      inputMap.put("last_name", "Damstra");
      inputMap.put("position_id", 3);
      inputMap.put("position_title", "VP Information Systems");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 2);
      inputMap.put("birth_date", "1942-10-08");
      inputMap.put("hire_date", "1994-12-01 00:00:00.0");
      inputMap.put("salary", 25000.0);
      inputMap.put("supervisor_id", 1);
      inputMap.put("education_level", "Bachelors Degree");
      inputMap.put("marital_status", "M");
      inputMap.put("gender", "F");
      inputMap.put("management_role", "Senior Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 7);
      inputMap.put("full_name", "Rebecca Kanagaki");
      inputMap.put("first_name", "Rebecca");
      inputMap.put("last_name", "Kanagaki");
      inputMap.put("position_id", 4);
      inputMap.put("position_title", "VP Human Resources");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 3);
      inputMap.put("birth_date", "1949-03-27");
      inputMap.put("hire_date", "1994-12-01 00:00:00.0");
      inputMap.put("salary", 15000.0);
      inputMap.put("supervisor_id", 1);
      inputMap.put("education_level", "Bachelors Degree");
      inputMap.put("marital_status", "M");
      inputMap.put("gender", "F");
      inputMap.put("management_role", "Senior Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 8);
      inputMap.put("full_name", "Kim Brunner");
      inputMap.put("first_name", "Kim");
      inputMap.put("last_name", "Brunner");
      inputMap.put("position_id", 11);
      inputMap.put("position_title", "Store Manager");
      inputMap.put("store_id", 9);
      inputMap.put("department_id", 11);
      inputMap.put("birth_date", "1922-08-10");
      inputMap.put("hire_date", "1998-01-01 00:00:00.0");
      inputMap.put("salary", 10000.0);
      inputMap.put("supervisor_id", 5);
      inputMap.put("education_level", "Bachelors Degree");
      inputMap.put("marital_status", "S");
      inputMap.put("gender", "F");
      inputMap.put("management_role", "Store Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 9);
      inputMap.put("full_name", "Brenda Blumberg");
      inputMap.put("first_name", "Brenda");
      inputMap.put("last_name", "Blumberg");
      inputMap.put("position_id", 11);
      inputMap.put("position_title", "Store Manager");
      inputMap.put("store_id", 21);
      inputMap.put("department_id", 11);
      inputMap.put("birth_date", "1979-06-23");
      inputMap.put("hire_date", "1998-01-01 00:00:00.0");
      inputMap.put("salary", 17000.0);
      inputMap.put("supervisor_id", 5);
      inputMap.put("education_level", "Graduate Degree");
      inputMap.put("marital_status", "M");
      inputMap.put("gender", "F");
      inputMap.put("management_role", "Store Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 10);
      inputMap.put("full_name", "Darren Stanz");
      inputMap.put("first_name", "Darren");
      inputMap.put("last_name", "Stanz");
      inputMap.put("position_id", 5);
      inputMap.put("position_title", "VP Finance");
      inputMap.put("store_id", 0);
      inputMap.put("department_id", 5);
      inputMap.put("birth_date", "1949-08-26");
      inputMap.put("hire_date", "1994-12-01 00:00:00.0");
      inputMap.put("salary", 50000.0);
      inputMap.put("supervisor_id", 1);
      inputMap.put("education_level", "Partial College");
      inputMap.put("marital_status", "M");
      inputMap.put("gender", "M");
      inputMap.put("management_role", "Senior Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }
    {
      Map<String, Object> inputMap = new HashMap<>();
      inputMap.put("employee_id", 11);
      inputMap.put("full_name", "Jonathan Murraiin");
      inputMap.put("first_name", "Jonathan");
      inputMap.put("last_name", "Murraiin");
      inputMap.put("position_id", 11);
      inputMap.put("position_title", "Store Manager");
      inputMap.put("store_id", 1);
      inputMap.put("department_id", 11);
      inputMap.put("birth_date", "1967-06-20");
      inputMap.put("hire_date", "1998-01-01 00:00:00.0");
      inputMap.put("salary", 15000.0);
      inputMap.put("supervisor_id", 5);
      inputMap.put("education_level", "Graduate Degree");
      inputMap.put("marital_status", "S");
      inputMap.put("gender", "M");
      inputMap.put("management_role", "Store Management");
      final Reader input1 = new StringReader(JSONObject.toJSONString(inputMap));

      IndexRequest<JsonData> request = IndexRequest.of(i -> i
          .index("employee")
          .withJson(input1)
      );
      elasticsearchClient.index(request);
    }

    RefreshRequest refreshRequest = new RefreshRequest.Builder()
        .index(indexNames)
        .build();
    elasticsearchClient.indices().refresh(refreshRequest);
  }

  @Test
  public void testSelectAll() throws Exception {
    testBuilder()
        .sqlQuery("select * from elastic.`employee`")
        .unOrdered()
        .baselineColumns("employee_id", "full_name", "first_name", "last_name", "position_id",
            "position_title", "store_id", "department_id", "birth_date", "hire_date", "salary",
            "supervisor_id", "education_level", "marital_status", "gender", "management_role",
          "binary_field", "boolean_field", "date_field", "byte_field", "long_field", "float_field",
          "short_field", "decimal_field")
        .baselineValues(1, "Sheri Nowmer", "Sheri", "Nowmer", 1, "President", 0, 1, "1961-08-26", "1994-12-01 00:00:00.0", 80000.0, 0, "Graduate Degree", "S", "F", "Senior Management", Base64.getEncoder().encodeToString("Senior Management".getBytes(StandardCharsets.UTF_8)), true, "2015/01/01 12:10:30", 123, 123, 123., 123, 123.45)
        .baselineValues(2, "Derrick Whelply", "Derrick", "Whelply", 2, "VP Country Manager", 0, 1, "1915-07-03", "1994-12-01 00:00:00.0", 40000.0, 1, "Graduate Degree", "M", "M", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(4, "Michael Spence", "Michael", "Spence", 2, "VP Country Manager", 0, 1, "1969-06-20", "1998-01-01 00:00:00.0", 40000.0, 1, "Graduate Degree", "S", "M", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(5, "Maya Gutierrez", "Maya", "Gutierrez", 2, "VP Country Manager", 0, 1, "1951-05-10", "1998-01-01 00:00:00.0", 35000.0, 1, "Bachelors Degree", "M", "F", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(6, "Roberta Damstra", "Roberta", "Damstra", 3, "VP Information Systems", 0, 2, "1942-10-08", "1994-12-01 00:00:00.0", 25000.0, 1, "Bachelors Degree", "M", "F", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(7, "Rebecca Kanagaki", "Rebecca", "Kanagaki", 4, "VP Human Resources", 0, 3, "1949-03-27", "1994-12-01 00:00:00.0", 15000.0, 1, "Bachelors Degree", "M", "F", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(8, "Kim Brunner", "Kim", "Brunner", 11, "Store Manager", 9, 11, "1922-08-10", "1998-01-01 00:00:00.0", 10000.0, 5, "Bachelors Degree", "S", "F", "Store Management", null, null, null, null, null, null, null, null)
        .baselineValues(9, "Brenda Blumberg", "Brenda", "Blumberg", 11, "Store Manager", 21, 11, "1979-06-23", "1998-01-01 00:00:00.0", 17000.0, 5, "Graduate Degree", "M", "F", "Store Management", null, null, null, null, null, null, null, null)
        .baselineValues(10, "Darren Stanz", "Darren", "Stanz", 5, "VP Finance", 0, 5, "1949-08-26", "1994-12-01 00:00:00.0", 50000.0, 1, "Partial College", "M", "M", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(11, "Jonathan Murraiin", "Jonathan", "Murraiin", 11, "Store Manager", 1, 11, "1967-06-20", "1998-01-01 00:00:00.0", 15000.0, 5, "Graduate Degree", "S", "M", "Store Management", null, null, null, null, null, null, null, null)
        .go();
  }

  @Test
  public void testSelectColumns() throws Exception {
    testBuilder()
        .sqlQuery("select full_name, birth_date from elastic.`employee`")
        .unOrdered()
        .baselineColumns("full_name", "birth_date")
        .baselineValues("Sheri Nowmer", "1961-08-26")
        .baselineValues("Derrick Whelply", "1915-07-03")
        .baselineValues("Michael Spence", "1969-06-20")
        .baselineValues("Maya Gutierrez", "1951-05-10")
        .baselineValues("Roberta Damstra", "1942-10-08")
        .baselineValues("Rebecca Kanagaki", "1949-03-27")
        .baselineValues("Kim Brunner", "1922-08-10")
        .baselineValues("Brenda Blumberg", "1979-06-23")
        .baselineValues("Darren Stanz", "1949-08-26")
        .baselineValues("Jonathan Murraiin", "1967-06-20")
        .go();
  }

  @Test
  public void testSelectAllFiltered() throws Exception {
    testBuilder()
        .sqlQuery("select * from elastic.`employee` where employee_id = 1 and first_name = 'sheri'")
        .unOrdered()
        .baselineColumns("employee_id", "full_name", "first_name", "last_name", "position_id",
            "position_title", "store_id", "department_id", "birth_date", "hire_date", "salary",
            "supervisor_id", "education_level", "marital_status", "gender", "management_role",
            "binary_field", "boolean_field", "date_field", "byte_field", "long_field", "float_field",
            "short_field", "decimal_field")
        .baselineValues(1, "Sheri Nowmer", "Sheri", "Nowmer", 1, "President", 0, 1, "1961-08-26",
            "1994-12-01 00:00:00.0", 80000.0, 0, "Graduate Degree", "S", "F", "Senior Management",
          Base64.getEncoder().encodeToString("Senior Management".getBytes(StandardCharsets.UTF_8)), true,
          "2015/01/01 12:10:30", 123, 123, 123., 123, 123.45)
        .go();
  }

  @Test
  public void testSelectColumnsFiltered() throws Exception {
    testBuilder()
        .sqlQuery("select full_name, birth_date from elastic.`employee` where employee_id = 1 and first_name = 'sheri'")
        .unOrdered()
        .baselineColumns("full_name", "birth_date")
        .baselineValues("Sheri Nowmer", "1961-08-26")
        .go();
  }

  @Test
  public void testSelectColumnsUnsupportedFilter() throws Exception {
    // Calcite doesn't support LIKE and other functions yet, so ensure Drill filter is used there
    testBuilder()
        .sqlQuery("select full_name, birth_date from elastic.`employee` where first_name like 'Sh%'")
        .unOrdered()
        .baselineColumns("full_name", "birth_date")
        .baselineValues("Sheri Nowmer", "1961-08-26")
        .go();
  }

  @Test
  public void testSelectColumnsUnsupportedProject() throws Exception {
    // Calcite doesn't support LIKE and other functions yet, so ensure Drill project is used there
    testBuilder()
        .sqlQuery("select first_name like 'Sh%' as c, birth_date from elastic.`employee` where first_name like 'Sh%'")
        .unOrdered()
        .baselineColumns("c", "birth_date")
        .baselineValues(true, "1961-08-26")
        .go();
  }

  @Test
  public void testSelectColumnsUnsupportedAggregate() throws Exception {
    // Calcite doesn't support stddev_samp and other functions yet, so ensure Drill agg is used there
    testBuilder()
        .sqlQuery("select stddev_samp(salary) as standard_deviation from elastic.`employee`")
        .unOrdered()
        .baselineColumns("standard_deviation")
        .baselineValues(21333.593748410563)
        .go();
  }

  @Test
  public void testLimitWithSort() throws Exception {
    testBuilder()
        .sqlQuery("select full_name, birth_date from elastic.`employee` order by employee_id limit 3")
        .ordered()
        .baselineColumns("full_name", "birth_date")
        .baselineValues("Sheri Nowmer", "1961-08-26")
        .baselineValues("Derrick Whelply", "1915-07-03")
        .baselineValues("Michael Spence", "1969-06-20")
        .go();
  }

  @Test
  public void testSelectAllWithLimitAndSort() throws Exception {
    testBuilder()
        .sqlQuery("select * from elastic.`employee` order by employee_id limit 3")
        .ordered()
        .baselineColumns("employee_id", "full_name", "first_name", "last_name", "position_id",
            "position_title", "store_id", "department_id", "birth_date", "hire_date", "salary",
            "supervisor_id", "education_level", "marital_status", "gender", "management_role",
          "binary_field", "boolean_field", "date_field", "byte_field", "long_field", "float_field",
          "short_field", "decimal_field")
        .baselineValues(1, "Sheri Nowmer", "Sheri", "Nowmer", 1, "President", 0, 1, "1961-08-26", "1994-12-01 00:00:00.0", 80000.0, 0, "Graduate Degree", "S", "F", "Senior Management", Base64.getEncoder().encodeToString("Senior Management".getBytes(StandardCharsets.UTF_8)), true, "2015/01/01 12:10:30", 123, 123, 123., 123, 123.45)
        .baselineValues(2, "Derrick Whelply", "Derrick", "Whelply", 2, "VP Country Manager", 0, 1, "1915-07-03", "1994-12-01 00:00:00.0", 40000.0, 1, "Graduate Degree", "M", "M", "Senior Management", null, null, null, null, null, null, null, null)
        .baselineValues(4, "Michael Spence", "Michael", "Spence", 2, "VP Country Manager", 0, 1, "1969-06-20", "1998-01-01 00:00:00.0", 40000.0, 1, "Graduate Degree", "S", "M", "Senior Management", null, null, null, null, null, null, null, null)
        .go();
  }

  @Test
  public void testSingleColumn() throws Exception {
    testBuilder()
        .sqlQuery("select full_name from elastic.`employee` order by employee_id limit 3")
        .ordered()
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .baselineValues("Derrick Whelply")
        .baselineValues("Michael Spence")
        .go();
  }

  @Test
  public void testJoin() throws Exception {
    testBuilder()
        .sqlQuery("select t1.full_name, t2.birth_date from elastic.`employee` t1 join elastic.`employee` t2 on t1.employee_id = t2.employee_id")
        .unOrdered()
        .baselineColumns("full_name", "birth_date")
        .baselineValues("Sheri Nowmer", "1961-08-26")
        .baselineValues("Derrick Whelply", "1915-07-03")
        .baselineValues("Michael Spence", "1969-06-20")
        .baselineValues("Maya Gutierrez", "1951-05-10")
        .baselineValues("Roberta Damstra", "1942-10-08")
        .baselineValues("Rebecca Kanagaki", "1949-03-27")
        .baselineValues("Kim Brunner", "1922-08-10")
        .baselineValues("Brenda Blumberg", "1979-06-23")
        .baselineValues("Darren Stanz", "1949-08-26")
        .baselineValues("Jonathan Murraiin", "1967-06-20")
        .go();
  }

  @Test
  public void testJoinWithFileTable() throws Exception {
    testBuilder()
        .sqlQuery("select t1.full_name, t2.birth_date from elastic.`employee` t1 join cp.`employee.json` t2 on t1.employee_id = t2.employee_id")
        .unOrdered()
        .baselineColumns("full_name", "birth_date")
        .baselineValues("Sheri Nowmer", "1961-08-26")
        .baselineValues("Derrick Whelply", "1915-07-03")
        .baselineValues("Michael Spence", "1969-06-20")
        .baselineValues("Maya Gutierrez", "1951-05-10")
        .baselineValues("Roberta Damstra", "1942-10-08")
        .baselineValues("Rebecca Kanagaki", "1949-03-27")
        .baselineValues("Kim Brunner", "1922-08-10")
        .baselineValues("Brenda Blumberg", "1979-06-23")
        .baselineValues("Darren Stanz", "1949-08-26")
        .baselineValues("Jonathan Murraiin", "1967-06-20")
        .go();
  }

  @Test
  public void testAggregate() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) c from elastic.`employee`")
        .ordered()
        .baselineColumns("c")
        .baselineValues(10L)
        .go();
  }

  @Test
  public void testAggregateWithGroupBy() throws Exception {
    testBuilder()
        .sqlQuery("select sum(`salary`) sal, department_id from elastic.`employee` e group by e.`department_id`")
        .unOrdered()
        .baselineColumns("sal", "department_id")
        .baselineValues(195000.0, 1)
        .baselineValues(42000.0, 11)
        .baselineValues(25000.0, 2)
        .baselineValues(15000.0, 3)
        .baselineValues(50000.0, 5)
        .go();
  }

  @Test
  public void testSelectNonExistingColumn() throws Exception {
    testBuilder()
        .sqlQuery("select full_name123 from elastic.`employee` order by employee_id limit 3")
        .unOrdered()
        .baselineColumns("full_name123")
        .baselineValuesForSingleColumn(null, null, null)
        .go();
  }

  @Test
  public void testSelectLiterals() throws Exception {
    testBuilder()
        .sqlQuery("select 'abc' as full_name, 123 as id from elastic.`employee` limit 3")
        .unOrdered()
        .baselineColumns("full_name", "id")
        .baselineValues("abc", 123)
        .baselineValues("abc", 123)
        .baselineValues("abc", 123)
        .go();
  }

  @Test
  public void testSelectIntLiterals() throws Exception {
    testBuilder()
        .sqlQuery("select 333 as full_name, 123 as id from elastic.`employee` limit 3")
        .unOrdered()
        .baselineColumns("full_name", "id")
        .baselineValues(333, 123)
        .baselineValues(333, 123)
        .baselineValues(333, 123)
        .go();
  }

  @Test
  public void testSelectLiteralWithStar() throws Exception {
    testBuilder()
        .sqlQuery("select *, 123 as full_name from elastic.`employee` order by employee_id limit 3")
        .unOrdered()
        .baselineColumns("full_name")
        .baselineColumns("employee_id", "full_name", "first_name", "last_name", "position_id",
            "position_title", "store_id", "department_id", "birth_date", "hire_date", "salary",
            "supervisor_id", "education_level", "marital_status", "gender", "management_role",
          "binary_field", "boolean_field", "date_field", "byte_field", "long_field", "float_field",
          "short_field", "decimal_field", "full_name0")
        .baselineValues(1, "Sheri Nowmer", "Sheri", "Nowmer", 1, "President", 0, 1, "1961-08-26", "1994-12-01 00:00:00.0", 80000.0, 0, "Graduate Degree", "S", "F", "Senior Management", Base64.getEncoder().encodeToString("Senior Management".getBytes(StandardCharsets.UTF_8)), true, "2015/01/01 12:10:30", 123, 123, 123., 123, 123.45, 123)
        .baselineValues(2, "Derrick Whelply", "Derrick", "Whelply", 2, "VP Country Manager", 0, 1, "1915-07-03", "1994-12-01 00:00:00.0", 40000.0, 1, "Graduate Degree", "M", "M", "Senior Management", null, null, null, null, null, null, null, null, 123)
        .baselineValues(4, "Michael Spence", "Michael", "Spence", 2, "VP Country Manager", 0, 1, "1969-06-20", "1998-01-01 00:00:00.0", 40000.0, 1, "Graduate Degree", "S", "M", "Senior Management", null, null, null, null, null, null, null, null, 123)
        .go();
  }

  @Test
  public void testLiteralWithAggregateAndGroupBy() throws Exception {
    testBuilder()
        .sqlQuery("select sum(`salary`) sal, 1 as department_id from elastic.`employee` e group by e.`department_id`")
        .unOrdered()
        .baselineColumns("sal", "department_id")
        .baselineValues(195000.0, 1)
        .baselineValues(42000.0, 1)
        .baselineValues(25000.0, 1)
        .baselineValues(15000.0, 1)
        .baselineValues(50000.0, 1)
        .go();
  }

  @Test
  public void testSelectNonExistingTable() throws Exception {
    try {
      queryBuilder().sql("select full_name from elastic.`non-existing`").run();
      fail("Query didn't fail");
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString("VALIDATION ERROR"));
      assertThat(e.getMessage(), containsString("Object 'non-existing' not found within 'elastic'"));
    }
  }

  @Test
  public void testSelectNonExistingSubSchema() throws Exception {
    try {
      queryBuilder().sql("select full_name from elastic.`non-existing`.employee").run();
      fail("Query didn't fail");
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString("VALIDATION ERROR"));
      assertThat(e.getMessage(), containsString("Object 'non-existing' not found within 'elastic'"));
    }
  }

  @Test
  public void testWithProvidedSchema() throws Exception {
    testBuilder()
        .sqlQuery("select * from " +
                "table(elastic.`employee`(schema=>'inline=(birth_date date not null, salary decimal(10, 2))')) " +
                "where employee_id = 1")
        .ordered()
        .baselineColumns("employee_id", "full_name", "first_name", "last_name", "position_id",
            "position_title", "store_id", "department_id", "birth_date", "hire_date", "salary",
            "supervisor_id", "education_level", "marital_status", "gender", "management_role",
          "binary_field", "boolean_field", "date_field", "byte_field", "long_field", "float_field",
          "short_field", "decimal_field")
        .baselineValues(1, "Sheri Nowmer", "Sheri", "Nowmer", 1, "President", 0, 1, LocalDate.parse("1961-08-26"),
            "1994-12-01 00:00:00.0", new BigDecimal("80000.00"), 0, "Graduate Degree", "S", "F", "Senior Management",
          Base64.getEncoder().encodeToString("Senior Management".getBytes(StandardCharsets.UTF_8)), true,
          "2015/01/01 12:10:30", 123, 123, 123., 123, 123.45)
        .go();
  }

  @Test
  public void testGroupByNonExistingColumn() throws Exception {
    try {
      queryBuilder().sql("select distinct(full_name123) from elastic.`employee`").run();
      fail("Query didn't fail");
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString("EXECUTION_ERROR ERROR: Field full_name123 not defined for employee"));
    }
  }
}
