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
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.util.ComparableVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assume.assumeTrue;

public class BaseCassandraTest extends ClusterTest {
  private static Cassandra cassandra;

  @BeforeClass
  public static void init() throws Exception {
    assumeTrue(
        "Skipping tests for JDK 12+ since Cassandra supports only versions up to 11 (including).",
        new ComparableVersion(System.getProperty("java.version"))
            .compareTo(new ComparableVersion("12")) < 0);

    startCluster(ClusterFixture.builder(dirTestWatcher));

    startCassandra();

    CassandraStorageConfig config = new CassandraStorageConfig(
        cassandra.getSettings().getAddress().getHostAddress(),
        cassandra.getSettings().getPort(),
        null,
        null);
    config.setEnabled(true);
    cluster.defineStoragePlugin("cassandra", config);

    prepareData();
  }

  @AfterClass
  public static void cleanUp() {
    if (cassandra != null) {
      cassandra.stop();
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
