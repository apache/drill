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
package org.apache.drill.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.drill.QueryTestUtil;
import org.apache.drill.TestBuilder;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.testing.Controls;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.apache.drill.test.ClusterFixture.FixtureTestServices;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSetBuilder;

/**
 * Represents a Drill client. Provides many useful test-specific operations such
 * as setting system options, running queries, and using the @{link TestBuilder}
 * class.
 * @see ExampleTest ExampleTest for usage examples
 */

public class ClientFixture implements AutoCloseable {

  public static class ClientBuilder {

    ClusterFixture cluster;
    Properties clientProps;

    protected ClientBuilder(ClusterFixture cluster) {
      this.cluster = cluster;
      clientProps = cluster.getClientProps();
    }

    /**
     * Specify an optional client property.
     * @param key property name
     * @param value property value
     * @return this builder
     */

    public ClientBuilder property(String key, Object value) {
      if (clientProps == null) {
        clientProps = new Properties();
      }
      clientProps.put(key, value);
      return this;
    }

    public ClientFixture build() {
      try {
        return new ClientFixture(this);
      } catch (RpcException e) {

        // When used in a test with an embedded Drillbit, the
        // RPC exception should not occur.

        throw new IllegalStateException(e);
      }
    }
  }

  private ClusterFixture cluster;
  private DrillClient client;

  public ClientFixture(ClientBuilder builder) throws RpcException {
    this.cluster = builder.cluster;

    // Create a client.

    if (cluster.usesZK()) {
      client = new DrillClient(cluster.config());
    } else {
      client = new DrillClient(cluster.config(), cluster.serviceSet().getCoordinator());
    }
    client.connect(builder.clientProps);
    cluster.clients.add(this);
  }

  public DrillClient client() { return client; }
  public ClusterFixture cluster() { return cluster; }
  public BufferAllocator allocator() { return client.getAllocator(); }

  /**
   * Set a runtime option.
   *
   * @param key
   * @param value
   * @throws RpcException
   */

  public void alterSession(String key, Object value) {
    String sql = "ALTER SESSION SET `" + key + "` = " + ClusterFixture.stringify(value);
    runSqlSilently(sql);
  }

  public void alterSystem(String key, Object value) {
    String sql = "ALTER SYSTEM SET `" + key + "` = " + ClusterFixture.stringify(value);
    runSqlSilently(sql);
  }

  /**
   * Run SQL silently (discard results.)
   *
   * @param sql
   * @throws RpcException
   */

  public void runSqlSilently(String sql) {
    try {
      queryBuilder().sql(sql).run();
    } catch (Exception e) {
      // Should not fail during tests. Convert exception to unchecked
      // to simplify test code.
      new IllegalStateException(e);
    }
  }

  public QueryBuilder queryBuilder() {
    return new QueryBuilder(this);
  }

  public int countResults(List<QueryDataBatch> results) {
    int count = 0;
    for(QueryDataBatch b : results) {
      count += b.getHeader().getRowCount();
    }
    return count;
  }

  public TestBuilder testBuilder() {
    return new TestBuilder(new FixtureTestServices(this));
  }

  /**
   * Run zero or more queries and optionally print the output in TSV format.
   * Similar to {@link QueryTestUtil#test}. Output is printed
   * only if the tests are running as verbose.
   *
   * @return the number of rows returned
   */

  public void runQueries(final String queryString) throws Exception{
    final String query = QueryTestUtil.normalizeQuery(queryString);
    String[] queries = query.split(";");
    for (String q : queries) {
      final String trimmedQuery = q.trim();
      if (trimmedQuery.isEmpty()) {
        continue;
      }
      queryBuilder().sql(trimmedQuery).print();
    }
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } finally {
      client = null;
      cluster.clients.remove(this);
    }
  }

  /**
   * Return a parsed query profile for a query summary. Saving of profiles
   * must be turned on.
   *
   * @param summary
   * @return
   * @throws IOException
   */

  public ProfileParser parseProfile(QuerySummary summary) throws IOException {
    return parseProfile(summary.queryIdString());
  }

  /**
   * Parse a query profile from the local storage location given the
   * query ID. Saving of profiles must be turned on. This is a bit of
   * a hack: the profile should be available directly from the server.
   * @throws IOException
   */

  public ProfileParser parseProfile(String queryId) throws IOException {
    File file = new File(cluster.getProfileDir(), queryId + ".sys.drill");
    return new ProfileParser(file);
  }

  /**
   * Set a set of injection controls that apply <b>on the next query
   * only</b>. That query should be your target query, but may
   * accidentally be an ALTER SESSION, EXPLAIN, etc. So, call this just
   * before the SELECT statement.
   *
   * @param controls the controls string created by
   * {@link Controls#newBuilder()} builder.
   */

  public void setControls(String controls) {
    ControlsInjectionUtil.validateControlsString(controls);
    alterSession(ExecConstants.DRILLBIT_CONTROL_INJECTIONS, controls);
  }

  public RowSetBuilder rowSetBuilder(BatchSchema schema) {
    return new RowSetBuilder(allocator(), schema);
  }
}
