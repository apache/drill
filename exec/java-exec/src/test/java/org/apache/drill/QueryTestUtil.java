/**
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
package org.apache.drill;

import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.util.VectorUtil;

/**
 * Utilities useful for tests that issue SQL queries.
 */
public class QueryTestUtil {
  /**
   * Constructor. All methods are static.
   */
  private QueryTestUtil() {
  }

  /**
   * Create a DrillClient that can be used to query a drill cluster.
   *
   * @param drillConfig
   * @param remoteServiceSet remote service set
   * @param maxWidth maximum width per node
   * @param props Connection properties contains properties such as "user", "password", "schema" etc
   * @return the newly created client
   * @throws RpcException if there is a problem setting up the client
   */
  public static DrillClient createClient(final DrillConfig drillConfig, final RemoteServiceSet remoteServiceSet,
      final int maxWidth, final Properties props) throws RpcException, OutOfMemoryException {
    final DrillClient drillClient = new DrillClient(drillConfig, remoteServiceSet.getCoordinator());
    drillClient.connect(props);

    final List<QueryDataBatch> results = drillClient.runQuery(
        QueryType.SQL, String.format("alter session set `%s` = %d",
            ExecConstants.MAX_WIDTH_PER_NODE_KEY, maxWidth));
    for (QueryDataBatch queryDataBatch : results) {
      queryDataBatch.release();
    }

    return drillClient;
  }

  /**
   * Normalize the query relative to the test environment.
   *
   * <p>Looks for "${WORKING_PATH}" in the query string, and replaces it the current
   * working patch obtained from {@link org.apache.drill.common.util.TestTools#getWorkingPath()}.
   *
   * @param query the query string
   * @return the normalized query string
   */
  public static String normalizeQuery(final String query) {
    if (query.contains("${WORKING_PATH}")) {
      return query.replaceAll(Pattern.quote("${WORKING_PATH}"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
    } else if (query.contains("[WORKING_PATH]")) {
      return query.replaceAll(Pattern.quote("[WORKING_PATH]"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
    }
    return query;
  }

  /**
   * Execute a SQL query, and print the results.
   *
   * @param drillClient drill client to use
   * @param type type of the query
   * @param queryString query string
   * @return number of rows returned
   * @throws Exception
   */
  public static int testRunAndPrint(
      final DrillClient drillClient, final QueryType type, final String queryString) throws Exception {
    final String query = normalizeQuery(queryString);
    PrintingResultsListener resultListener =
        new PrintingResultsListener(drillClient.getConfig(), Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
    drillClient.runQuery(type, query, resultListener);
    return resultListener.await();
  }

  /**
   * Execute one or more queries separated by semicolons, and print the results.
   *
   * @param drillClient drill client to use
   * @param queryString the query string
   * @throws Exception
   */
  public static void test(final DrillClient drillClient, final String queryString) throws Exception{
    final String query = normalizeQuery(queryString);
    String[] queries = query.split(";");
    for (String q : queries) {
      final String trimmedQuery = q.trim();
      if (trimmedQuery.isEmpty()) {
        continue;
      }
      testRunAndPrint(drillClient, QueryType.SQL, trimmedQuery);
    }
  }

  /**
   * Execute one or more queries separated by semicolons, and print the results, with the option to
   * add formatted arguments to the query string.
   *
   * @param drillClient drill client to use
   * @param query the query string; may contain formatting specifications to be used by
   *   {@link String#format(String, Object...)}.
   * @param args optional args to use in the formatting call for the query string
   * @throws Exception
   */
  public static void test(final DrillClient drillClient, final String query, Object... args) throws Exception {
    test(drillClient, String.format(query, args));
  }

  /**
   * Execute a single query with a user supplied result listener.
   *
   * @param drillClient drill client to use
   * @param type type of query
   * @param queryString the query string
   * @param resultListener the result listener
   */
  public static void testWithListener(final DrillClient drillClient, final QueryType type,
      final String queryString, final UserResultsListener resultListener) {
    final String query = QueryTestUtil.normalizeQuery(queryString);
    drillClient.runQuery(type, query, resultListener);
  }
}
