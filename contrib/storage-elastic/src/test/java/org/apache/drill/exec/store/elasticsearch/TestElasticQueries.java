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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


@Ignore("It requires an elasticsearch server running on localhost, port 9200 with init-script.sh script run on it")
public class TestElasticQueries extends ClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(TestElasticQueries.class);

    @BeforeClass
    public static void setup() throws Exception {
        startCluster(ClusterFixture.builder(dirTestWatcher));

        StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();

        ElasticSearchPluginConfig esConfig = new ElasticSearchPluginConfig("elastic:changeme", "http://localhost:9200", "", 100, 10, TimeUnit.MINUTES);
        esConfig.setEnabled(true);

        pluginRegistry.createOrUpdate("elasticsearch", esConfig, true);
    }

    @Test
    public void testGetAllDevelopers() throws Exception {
        String sql = String.format(ElasticSearchTestConstants.TEST_SELECT_ALL_QUERY_TEMPLATE, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
        logger.debug("ES Query: {}", sql);

        RowSet results = client.queryBuilder().sql(sql).rowSet();
        logger.debug("Query Results: {}", results.toString());

    /*TupleMetadata expectedSchema = new SchemaBuilder()
      .add("SCHEMA_NAME", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("TYPE", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("api.mock", "http")
      .addRow("api.mockpost", "http")
      .addRow("api.stock", "http")
      .addRow("api.sunrise", "http")
      .addRow("api", "http")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);*/
    }

  /*
  @Test
  public void testGetAllDevelopers() throws Exception {
    String sql = String.format(ElasticSearchTestConstants.TEST_SELECT_ALL_QUERY_TEMPLATE, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
    runElasticSearchSQLVerifyCount(queryString, 19);
  }

  @Test
  public void testGetAllDevelopersIDsAndNames() throws Exception {
    String queryString = String.format(ElasticSearchTestConstants.TEST_SELECT_IDNAMES_QUERY_TEMPLATE, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
    runElasticSearchSQLVerifyCount(queryString, 19);
  }

  //@Test
  public void testBooleanFilter() throws Exception {
    String queryString = String.format(ElasticSearchTestConstants.TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
    runElasticSearchSQLVerifyCount(queryString, 19);
    queryString = String.format(ElasticSearchTestConstants.TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
    runElasticSearchSQLVerifyCount(queryString, 8);

  }*/
}
