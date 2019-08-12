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

package org.apache.drill.exec.udfs;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({UnlikelyTest.class, SqlFunctionTest.class})

public class TestProtocolFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testServiceName() throws Exception {
    String query = "select get_service_name(443,'tcp') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("http protocol over TLS/SSL").go();

    query = "select get_service_name(34, 'sdfsdfafsdfadfdsf') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("Unknown").go();

    query = "select get_service_name( cast(null as INTEGER),'') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();

    query = "select get_service_name(43, cast(null as varchar)) as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();
  }

  @Test
  public void testStringServiceName() throws Exception {
    String query = "select get_service_name('443','tcp') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("http protocol over TLS/SSL").go();

    query = "select get_service_name('34', 'sdfsdfafsdfadfdsf') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("Unknown").go();

    query = "select get_service_name( cast(null as VARCHAR),'') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();

    query = "select get_service_name('43', cast(null as varchar)) as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();
  }


  @Test
  public void testShortServiceName() throws Exception {
    String query = "select get_short_service_name(443,'tcp') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("https").go();

    query = "select get_short_service_name(34, 'sdfsdfafsdfadfdsf') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("Unknown").go();

    query = "select get_short_service_name( cast(null as INTEGER),'') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();

    query = "select get_short_service_name(43, cast(null as varchar)) as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();
  }

  @Test
  public void testStringShortServiceName() throws Exception {
    String query = "select get_short_service_name('666','tcp') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("doom").go();

    query = "select get_short_service_name('34', 'sdfsdfafsdfadfdsf') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues("Unknown").go();

    query = "select get_short_service_name( cast(null as varchar),'') as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();

    query = "select get_short_service_name('43', cast(null as varchar)) as service from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("service").baselineValues((String)null).go();
  }
}
