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

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.store.elasticsearch.ElasticSearchQueryTest.prepareData;
import static org.junit.Assert.assertEquals;

public class ElasticSearchUserTranslationTest extends ClusterTest {
  public static RestHighLevelClient restHighLevelClient;

  private static String indexName;

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
    credentialsProvider.setUserCredentials("admin", "password", TEST_USER_1);
    // Add unauthorized user
    credentialsProvider.setUserCredentials("nope", "no way dude", TEST_USER_2);


    ElasticsearchStorageConfig config = new ElasticsearchStorageConfig(
        Collections.singletonList(TestElasticsearchSuite.getAddress()),
        null, null, null, AuthMode.SHARED_USER.name(), PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    config.setEnabled(true);
    cluster.defineStoragePlugin("elastic", config);

    ElasticsearchStorageConfig ut_config = new ElasticsearchStorageConfig(
        Collections.singletonList(TestElasticsearchSuite.getAddress()),
        null, null, null, AuthMode.USER_TRANSLATION.name(), credentialsProvider);
    config.setEnabled(true);
    cluster.defineStoragePlugin("ut_elastic", config);

    prepareData();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    restHighLevelClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
    TestElasticsearchSuite.tearDownCluster();
  }

  @Test
  public void testInfoSchemaQueryWithMissingCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to Splunk.
    // This user should not see the ut_elastic because they do not have valid credentials
    ClientFixture client = cluster
        .clientBuilder()
        .property(DrillProperties.USER, ADMIN_USER)
        .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
        .build();

    String sql = "SHOW DATABASES WHERE schema_name LIKE '%elastic%'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
  }

}
