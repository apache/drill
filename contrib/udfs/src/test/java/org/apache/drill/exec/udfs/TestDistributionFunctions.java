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

import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDistributionFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testWidthBucket() throws Exception {
    // Test with float input
    String query = "SELECT width_bucket(5.35, 0,10,5) AS bucket FROM (VALUES(1))";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("bucket")
        .baselineValues(3)
        .go();

    // Test with int input
    query = "SELECT width_bucket(2, 0,10,5) AS bucket FROM (VALUES(1))";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("bucket")
        .baselineValues(2)
        .go();

    // Test with string input
    query = "SELECT width_bucket('9', 0,10,5) AS bucket FROM (VALUES(1))";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("bucket")
        .baselineValues(5)
        .go();

    // Test with input out of range
    query = "SELECT width_bucket(-5, 0,10,5) AS too_low_bucket, width_bucket(505, 0,10,5) AS too_high_bucket FROM (VALUES(1))";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("too_low_bucket", "too_high_bucket")
        .baselineValues(0, 6)
        .go();

  }

  @Test
  public void testKendall() throws Exception {
    String query = "SELECT kendall_correlation(col1,col2) AS R FROM cp.`test_data.csvh`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("R")
        .baselineValues(0.16666666666666666)
        .go();

  }

  @Test
  public void testRegrSlope() throws Exception {
    String query = "SELECT regr_slope(spend,sales) AS slope FROM cp.`regr_test.csvh`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("slope")
        .baselineValues(10.619633290847284)
        .go();
  }

  @Test
  public void testRegrIntercept() throws Exception {
    String query = "SELECT regr_intercept(spend,sales) AS intercept FROM cp.`regr_test.csvh`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("intercept")
        .baselineValues(1400.2322223740048)
        .go();
  }
}
