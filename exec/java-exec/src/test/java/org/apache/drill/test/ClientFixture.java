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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
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
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClientFixture.class);

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
    } else if (builder.clientProps != null  &&
        builder.clientProps.containsKey(DrillProperties.DRILLBIT_CONNECTION)) {
      client = new DrillClient(cluster.config(), cluster.serviceSet().getCoordinator(), true);
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
   * Reset a system option
   * @param key
   */

  public void resetSession(String key) {
    runSqlSilently("ALTER SESSION RESET `" + key + "`");
  }

  public void resetSystem(String key) {
    runSqlSilently("ALTER SYSTEM RESET `" + key + "`");
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
   * Run zero or more queries and output the results in TSV format.
   */
  private void runQueriesAndOutput(final String queryString,
                                   final boolean print) throws Exception {
    final String query = QueryTestUtil.normalizeQuery(queryString);
    String[] queries = query.split(";");
    for (String q : queries) {
      final String trimmedQuery = q.trim();
      if (trimmedQuery.isEmpty()) {
        continue;
      }

      if (print) {
        queryBuilder().sql(trimmedQuery).print();
      } else {
        queryBuilder().sql(trimmedQuery).log();
      }
    }
  }

  /**
   * Run zero or more queries and log the output in TSV format.
   */
  public void runQueriesAndLog(final String queryString) throws Exception {
    runQueriesAndOutput(queryString, false);
  }

  /**
   * Run zero or more queries and print the output in TSV format.
   */
  public void runQueriesAndPrint(final String queryString) throws Exception {
    runQueriesAndOutput(queryString, true);
  }

  /**
   * Plan a query without execution.
   * @throws ExecutionException
   * @throws InterruptedException
   */

  public QueryPlanFragments planQuery(QueryType type, String query, boolean isSplitPlan) {
    DrillRpcFuture<QueryPlanFragments> queryFragmentsFutures = client.planQuery(type, query, isSplitPlan);
    try {
      return queryFragmentsFutures.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  public QueryPlanFragments planQuery(String sql) {
    return planQuery(QueryType.SQL, sql, false);
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
   * {@link org.apache.drill.exec.testing.Controls#newBuilder()} builder.
   */

  public void setControls(String controls) {
    ControlsInjectionUtil.validateControlsString(controls);
    alterSession(ExecConstants.DRILLBIT_CONTROL_INJECTIONS, controls);
  }

  public RowSetBuilder rowSetBuilder(BatchSchema schema) {
    return new RowSetBuilder(allocator(), schema);
  }

  public RowSetBuilder rowSetBuilder(TupleMetadata schema) {
    return new RowSetBuilder(allocator(), schema);
  }

  /**
   * Very simple parser for semi-colon separated lists of SQL statements which
   * handles quoted semicolons. Drill can execute only one statement at a time
   * (without a trailing semi-colon.) This parser breaks up a statement list
   * into single statements. Input:<code><pre>
   * USE a.b;
   * ALTER SESSION SET `foo` = ";";
   * SELECT * FROM bar WHERE x = "\";";
   * </pre><code>Output:
   * <ul>
   * <li><tt>USE a.b</tt></li>
   * <li><tt>ALTER SESSION SET `foo` = ";"</tt></li>
   * <li><tt>SELECT * FROM bar WHERE x = "\";"</tt></li>
   */

  public static class StatementParser {
    private final Reader in;
    private StringBuilder buf;

    public StatementParser(Reader in) {
      this.in = in;
    }

    public String parseNext() throws IOException {
      boolean eof = false;
      buf = new StringBuilder();
      for (;;) {
        int c = in.read();
        if (c == -1) {
          eof = true;
          break;
        }
        if (c == ';') {
          break;
        }
        buf.append((char) c);
        if (c == '"' || c == '\'' || c == '`') {
          int quote = c;
          boolean escape = false;
          for (;;) {
            c = in.read();
            if (c == -1) {
              throw new IllegalArgumentException("Mismatched quote: " + (char) c);
            }
            buf.append((char) c);
            if (! escape && c == quote) {
              break;
            }
            escape = c == '\\';
          }
        }
      }
      String stmt = buf.toString().trim();
      if (stmt.isEmpty() && eof) {
        return null;
      }
      return stmt;
    }
  }

  public int exec(Reader in) throws IOException {
    StatementParser parser = new StatementParser(in);
    int count = 0;
    for (;;) {
      String stmt = parser.parseNext();
      if (stmt == null) {
        logger.debug("----");
        return count;
      }
      if (stmt.isEmpty()) {
        continue;
      }

      logger.debug("----");
      logger.debug(stmt);
      runSqlSilently(stmt);
      count++;
    }
  }

  /**
   * Execute a set of statements from a file.
   * @param source the set of statements, separated by semicolons
   * @return the number of statements executed
   */

  public int exec(File source) throws FileNotFoundException, IOException {
    try (Reader in = new BufferedReader(new FileReader(source))) {
      return exec(in);
    }
  }

  /**
   * Execute a set of statements from a string.
   * @param stmts the set of statements, separated by semicolons
   * @return the number of statements executed
   */

  public int exec(String stmts) {
    try (Reader in = new StringReader(stmts)) {
      return exec(in);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
