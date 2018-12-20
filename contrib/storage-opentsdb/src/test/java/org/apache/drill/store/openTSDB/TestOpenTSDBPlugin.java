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
package org.apache.drill.store.openTSDB;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePluginConfig;
import org.apache.drill.test.QueryTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.apache.drill.store.openTSDB.TestDataHolder.DOWNSAMPLE_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.DOWNSAMPLE_REQUEST_WTIHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.END_PARAM_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.END_PARAM_REQUEST_WTIHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.POST_REQUEST_WITHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.POST_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.REQUEST_TO_NONEXISTENT_METRIC;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_GET_TABLE_NAME_REQUEST;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_GET_TABLE_REQUEST;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_END_REQUEST_WITHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_END_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_REQUEST_WITH_TAGS;

public class TestOpenTSDBPlugin extends PlanTestBase {

  private static int portNumber;

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(portNumber);

  @BeforeClass
  public static void setup() throws Exception {
    portNumber = QueryTestUtil.getFreePortNumber(10_000, 200);
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    OpenTSDBStoragePluginConfig storagePluginConfig =
        new OpenTSDBStoragePluginConfig(String.format("http://localhost:%s", portNumber));
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(OpenTSDBStoragePluginConfig.NAME, storagePluginConfig, true);
  }

  @Before
  public void init() {
    setupPostStubs();
    setupGetStubs();
  }

  private void setupGetStubs() {
    wireMockRule.stubFor(get(urlEqualTo("/api/suggest?type=metrics&max=" + Integer.MAX_VALUE))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_GET_TABLE_NAME_REQUEST)));

    wireMockRule.stubFor(get(urlEqualTo("/api/query?start=47y-ago&m=sum:warp.speed.test"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(SAMPLE_DATA_FOR_GET_TABLE_REQUEST)
        ));
  }

  private void setupPostStubs() {
    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(POST_REQUEST_WITHOUT_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_GET_TABLE_REQUEST)));

    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(POST_REQUEST_WITH_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_POST_REQUEST_WITH_TAGS)));

    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(DOWNSAMPLE_REQUEST_WTIHOUT_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITHOUT_TAGS)));

    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
            .withRequestBody(equalToJson(END_PARAM_REQUEST_WTIHOUT_TAGS))
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(SAMPLE_DATA_FOR_POST_END_REQUEST_WITHOUT_TAGS)));

    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(DOWNSAMPLE_REQUEST_WITH_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITH_TAGS)));

    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
            .withRequestBody(equalToJson(END_PARAM_REQUEST_WITH_TAGS))
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(SAMPLE_DATA_FOR_POST_END_REQUEST_WITH_TAGS)));

    wireMockRule.stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(REQUEST_TO_NONEXISTENT_METRIC))
        .willReturn(aResponse()
            .withStatus(400)
            .withHeader("Content-Type", "application/json")
        ));
  }

  @Test
  public void testBasicQueryFromWithRequiredParams() throws Exception {
    String query =
            "select * from openTSDB.`(metric=warp.speed.test, start=47y-ago, aggregator=sum)`";
    Assert.assertEquals(18, testSql(query));
  }

  @Test
  public void testBasicQueryGroupBy() throws Exception {
    String query =
            "select `timestamp`, sum(`aggregated value`) from openTSDB.`(metric=warp.speed.test, aggregator=sum, start=47y-ago)` group by `timestamp`";
    Assert.assertEquals(15, testSql(query));
  }

  @Test
  public void testBasicQueryFromWithInterpolationParam() throws Exception {
    String query = "select * from openTSDB.`(metric=warp.speed.test, aggregator=sum, start=47y-ago, downsample=5y-avg)`";
    Assert.assertEquals(4, testSql(query));
  }

  @Test
  public void testBasicQueryFromWithEndParam() throws Exception {
    String query = "select * from openTSDB.`(metric=warp.speed.test, aggregator=sum, start=47y-ago, end=1407165403000))`";
    Assert.assertEquals(5, testSql(query));
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithoutTableName() throws Exception {
    test("select * from openTSDB.``;");
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithNonExistentTableName() throws Exception {
    test("select * from openTSDB.`warp.spee`");
  }

  @Test
  public void testPhysicalPlanSubmission() throws Exception {
    String query = "select * from openTSDB.`(metric=warp.speed.test, start=47y-ago, aggregator=sum)`";
    testPhysicalPlanExecutionBasedOnQuery(query);
  }

  @Test
  public void testDescribe() throws Exception {
    test("use openTSDB");
    test("describe `warp.speed.test`");
    Assert.assertEquals(1, testSql("show tables"));
  }
}
