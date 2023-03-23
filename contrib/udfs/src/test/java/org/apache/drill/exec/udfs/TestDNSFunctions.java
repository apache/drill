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
public class TestDNSFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testGetHostAddress() throws Exception {
    String query = "select get_host_address('apache.org') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("151.101.2.132").go();

    query = "select get_host_address('google') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("Unknown").go();

    query = "select get_host_address('') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("127.0.0.1").go();

    query = "select get_host_address(cast(null as varchar)) as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues((String)null).go();
  }

  @Test
  public void testGetHostName() throws Exception {
    String query = "select get_host_name('142.251.16.102') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("bl-in-f102.1e100.net").go();

    query = "select get_host_name('sdfsdfafsdfadfdsf') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("Unknown host").go();

    query = "select get_host_name('') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("localhost").go();

    query = "select get_host_name(cast(null as varchar)) as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues((String)null).go();
  }

  @Test
  public void testDNSLookup() throws Exception {
    String sql = "SELECT dns_lookup('google.com') AS dns_info FROM (VALUES(1))";
    testBuilder().sqlQuery(sql).ordered().baselineColumns("dns_info").go();
  }

  @Test
  public void testWhois() throws Exception {
    String sql = "SELECT whois('google.com') AS whois FROM (VALUES(1))";
    testBuilder().sqlQuery(sql).ordered().baselineColumns("whois").go();
  }
}
