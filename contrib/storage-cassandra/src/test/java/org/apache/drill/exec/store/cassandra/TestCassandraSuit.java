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
package org.apache.drill.exec.store.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;
import com.github.nosan.embedded.cassandra.cql.CqlScript;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.test.BaseTest;
import org.apache.hadoop.util.ComparableVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assume.assumeTrue;

@Category(SlowTest.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({CassandraComplexTypesTest.class, CassandraPlanTest.class, CassandraQueryTest.class})
public class TestCassandraSuit extends BaseTest {

  private static final AtomicInteger initCount = new AtomicInteger(0);

  protected static Cassandra cassandra;

  private static volatile boolean runningSuite = false;

  @BeforeClass
  public static void initCassandra() {
    assumeTrue(
        "Skipping tests for JDK 12+ since Cassandra supports only versions up to 11 (including).",
        new ComparableVersion(System.getProperty("java.version"))
            .compareTo(new ComparableVersion("12")) < 0);
    synchronized (TestCassandraSuit.class) {
      if (initCount.get() == 0) {
        startCassandra();
        prepareData();
      }
      initCount.incrementAndGet();
      runningSuite = true;
    }
  }

  public static boolean isRunningSuite() {
    return runningSuite;
  }

  @AfterClass
  public static void tearDownCluster() {
    synchronized (TestCassandraSuit.class) {
      if (initCount.decrementAndGet() == 0 && cassandra != null) {
        cassandra.stop();
      }
    }
  }

  private static void startCassandra() {
    cassandra = new CassandraBuilder().build();
    cassandra.start();
  }

  private static void prepareData() {
    Settings settings = cassandra.getSettings();

    try (Cluster cluster = Cluster.builder()
        .addContactPoints(settings.getAddress())
        .withPort(settings.getPort())
        .withoutMetrics()
        .withoutJMXReporting()
        .build()) {
      Session session = cluster.connect();
      CqlScript.ofClassPath("queries.cql").forEachStatement(session::execute);
    }
  }
}
