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

import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


@Ignore("It requires an elasticsearch server running on localhost, port 9200 with init-script.sh script run on it")
public class TestElasticQueries extends ClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(TestElasticQueries.class);

    @BeforeClass
    public static void setup() throws Exception {
        startCluster(ClusterFixture.builder(dirTestWatcher));

        StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();

        ElasticSearchPluginConfig esConfig = new ElasticSearchPluginConfig("elastic:changeme", "http://localhost:9200", "", 10000, 10, TimeUnit.MINUTES);
        esConfig.setEnabled(true);

        pluginRegistry.createOrUpdate("elasticsearch", esConfig, true);
    }

    @Test
    public void testSimpleStarQuery() throws Exception {
        String sql = String.format(ElasticSearchTestConstants.TEST_SELECT_ALL_QUERY_TEMPLATE,
          ElasticSearchTestConstants.EMPLOYEE_IDX,
          ElasticSearchTestConstants.DEVELOPER_MAPPING);

        logger.debug(sql);

        testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("name", "employeeId", "department", "reportsTo", "_id")
          .baselineValues("developer2", 3L, "IT", "manager1", "developer02")
          .baselineValues("developer5", 5L, "IT", "manager1", "developer05")
          .baselineValues("developer8", 8L, "IT", "manager1", "developer08")
          .baselineValues("developer12", 12L, "IT", "manager1", "developer12")
          .baselineValues("developer16", 17L, "IT", "manager2", "developer16")
          .baselineValues("developer1", 2L, "IT", "manager1", "developer01")
          .baselineValues("developer13", 13L, "IT", "manager1", "developer13")
          .baselineValues("developer14", 14L, "IT", "manager1", "developer14")
          .baselineValues("developer15", 15L, "IT", "manager1", "developer15")
          .baselineValues("developer17", 18L, "IT", "manager2", "developer17")
          .baselineValues("developer19", 20L, "IT", "manager2", "developer19")
          .baselineValues("developer4", 4L, "IT", "manager1", "developer04")
          .baselineValues("developer6", 6L, "IT", "manager1", "developer06")
          .baselineValues("developer7", 7L, "IT", "manager1", "developer07")
          .baselineValues("developer9", 9L, "IT", "manager1", "developer09")
          .baselineValues("developer10", 10L, "IT", "manager1", "developer10")
          .baselineValues("developer11", 11L, "IT", "manager1", "developer11")
          .baselineValues("developer18", 19L, "IT", "manager2", "developer18")
          .baselineValues("developer20", 21L, "IT", "manager2", "developer20")
          .go();
    }

    @Test
    public void testSimpleExplicitAllFieldsQuery() throws Exception {
        String sql = "SELECT _id, `name`, employeeId, department, reportsTo FROM elasticsearch.employee.`developer`";

        testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("name", "employeeId", "department", "reportsTo", "_id")
          .baselineValues("developer2", 3L, "IT", "manager1", "developer02")
          .baselineValues("developer5", 5L, "IT", "manager1", "developer05")
          .baselineValues("developer8", 8L, "IT", "manager1", "developer08")
          .baselineValues("developer12", 12L, "IT", "manager1", "developer12")
          .baselineValues("developer16", 17L, "IT", "manager2", "developer16")
          .baselineValues("developer1", 2L, "IT", "manager1", "developer01")
          .baselineValues("developer13", 13L, "IT", "manager1", "developer13")
          .baselineValues("developer14", 14L, "IT", "manager1", "developer14")
          .baselineValues("developer15", 15L, "IT", "manager1", "developer15")
          .baselineValues("developer17", 18L, "IT", "manager2", "developer17")
          .baselineValues("developer19", 20L, "IT", "manager2", "developer19")
          .baselineValues("developer4", 4L, "IT", "manager1", "developer04")
          .baselineValues("developer6", 6L, "IT", "manager1", "developer06")
          .baselineValues("developer7", 7L, "IT", "manager1", "developer07")
          .baselineValues("developer9", 9L, "IT", "manager1", "developer09")
          .baselineValues("developer10", 10L, "IT", "manager1", "developer10")
          .baselineValues("developer11", 11L, "IT", "manager1", "developer11")
          .baselineValues("developer18", 19L, "IT", "manager2", "developer18")
          .baselineValues("developer20", 21L, "IT", "manager2", "developer20")
          .go();
    }

    @Test
    public void testSimpleExplicitSomeFieldsQuery() throws Exception {
        String sql = "SELECT `name`, employeeId FROM elasticsearch.employee.`developer`";

        testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns( "name", "employeeId")
          .baselineValues("developer2", 3L)
          .baselineValues("developer5", 5L)
          .baselineValues("developer8", 8L)
          .baselineValues("developer12", 12L)
          .baselineValues("developer16", 17L)
          .baselineValues("developer1", 2L)
          .baselineValues("developer13", 13L)
          .baselineValues("developer14", 14L)
          .baselineValues("developer15", 15L)
          .baselineValues("developer17", 18L)
          .baselineValues("developer19", 20L)
          .baselineValues("developer4", 4L)
          .baselineValues("developer6", 6L)
          .baselineValues("developer7", 7L)
          .baselineValues("developer9", 9L)
          .baselineValues("developer10", 10L)
          .baselineValues("developer11", 11L)
          .baselineValues("developer18", 19L)
          .baselineValues("developer20", 21L)
          .go();
    }

    @Test
    public void testSerDe() throws Exception {
        String sql = "SELECT COUNT(*) FROM elasticsearch.employee.`developer`";
        String plan = queryBuilder().sql(sql).explainJson();
        long cnt = queryBuilder().physical(plan).singletonLong();
        assertEquals("Counts should match",19L, cnt);
    }

    @Test
    public void testGreaterThanFilterQuery() throws Exception {
        String sql = "SELECT `name`, employeeId FROM elasticsearch.employee.`developer` WHERE employeeID > 19";
        testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns( "name", "employeeId")
          .baselineValues("developer19", 20L)
          .baselineValues("developer20", 21L)
          .go();
    }


    @Test
    public void testSimpleExplicitAllDocumentQuery() throws Exception {
            String sql = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE TYPE='elasticsearch'";

            RowSet results = client.queryBuilder().sql(sql).rowSet();
            logger.debug("Query Results: {}", results.toString());

            results.print();
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

  //@Test
  public void testBooleanFilter() throws Exception {
    String queryString = String.format(ElasticSearchTestConstants.TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
    runElasticSearchSQLVerifyCount(queryString, 19);
    queryString = String.format(ElasticSearchTestConstants.TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2, ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
    runElasticSearchSQLVerifyCount(queryString, 8);
  }*/
}
