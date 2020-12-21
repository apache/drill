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
public class TestThreatHuntingFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testPunctuationPattern() throws Exception {
    String query = "SELECT punctuation_pattern('192.168.1.1 - - [10/Oct/2020:12:32:27 +0000] \"GET /some/web/app?param=test&param2=another_test\" 200 9987') AS pp FROM (VALUES" +
      "(1))";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("pp")
      .baselineValues("..._-_-_[//:::_+]_\"_///?=&=_\"__")
      .go();
  }

  @Test
  public void testEmptyPunctuationPattern() throws Exception {
    String query = "SELECT punctuation_pattern('') AS pp FROM (VALUES(1))";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("pp")
      .baselineValues("")
      .go();
  }

  @Test
  public void testEntropyFunction() throws Exception {
    String query = "SELECT entropy('asdkjflkdsjlefjdc') AS entropy FROM (VALUES(1))";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("entropy")
      .baselineValues(3.057476076289932)
      .go();
  }

  @Test
  public void testEntropyFunctionWithEmptyString() throws Exception {
    String query = "SELECT entropy('') AS entropy FROM (VALUES(1))";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("entropy")
      .baselineValues(0.0)
      .go();
  }

  @Test
  public void testNormedEntropyFunction() throws Exception {
    String query = "SELECT entropy_per_byte('asdkjflkdsjlefjdc') AS entropy FROM (VALUES(1))";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("entropy")
      .baselineValues(0.17985153389940778)
      .go();
  }

  @Test
  public void testNormedEntropyFunctionWithEmptyString() throws Exception {
    String query = "SELECT entropy_per_byte('') AS entropy FROM (VALUES(1))";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("entropy")
      .baselineValues(0.0)
      .go();
  }

}

