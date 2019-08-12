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
import java.util.ArrayList;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestDNSFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testGetHostAddress() throws Exception {
    String query = "select get_host_address('gtkcyber.com') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("216.239.36.21").go();

    query = "select get_host_address('google') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("Unknown").go();

    query = "select get_host_address('') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("127.0.0.1").go();

    query = "select get_host_address(cast(null as varchar)) as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues((String)null).go();
  }

  @Test
  public void testGetHostName() throws Exception {
    String query = "select get_host_name('216.239.36.21') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("any-in-2415.1e100.net").go();

    query = "select get_host_name('sdfsdfafsdfadfdsf') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("Unknown host").go();

    query = "select get_host_name('') as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues("localhost").go();

    query = "select get_host_name(cast(null as varchar)) as hostname from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("hostname").baselineValues((String)null).go();
  }

  @Test
  public void testGetMXName() throws Exception {
    String query = "select get_mx_record('gmail.com') as record from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues("gmail-smtp-in.l.google.com.").go();

    query = "select get_mx_record('sdfsdfafsdfadfdsf') as record from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues("MX Record not found").go();

    query = "select get_mx_record('') as record from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues("MX Record not found").go();

    query = "select get_mx_record(cast(null as varchar)) as record from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues((String)null).go();
  }

  @Test
  public void testGetMXNames() throws Exception {
    String query =  "select flatten(get_mx_records('gmail.com')) AS mx_records FROM (VALUES(1)) order by mx_records ASC";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("mx_records")
      .baselineValues("alt1.gmail-smtp-in.l.google.com.")
      .baselineValues("alt2.gmail-smtp-in.l.google.com.")
      .baselineValues("alt3.gmail-smtp-in.l.google.com.")
      .baselineValues("alt4.gmail-smtp-in.l.google.com.")
      .baselineValues("gmail-smtp-in.l.google.com.")
      .go();

    query = "select get_mx_records('sdfsdfafsdfadfdsf') as record from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues((String)null).go();

    query = "select get_mx_records('') as record from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues((String)null).go();

    //query = "select get_mx_records(cast(null as varchar)) as record from (values(1))";
    //testBuilder().sqlQuery(query).ordered().baselineColumns("record").baselineValues((String)null).go();
  }
}