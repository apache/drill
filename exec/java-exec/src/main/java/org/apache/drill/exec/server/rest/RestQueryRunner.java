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
package org.apache.drill.exec.server.rest;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.QueryResultsMode;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.InboundImpersonationManager;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.SchemaTreeProvider;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.WorkManager;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestQueryRunner {
  private static final Logger logger = LoggerFactory.getLogger(QueryWrapper.class);
  private static final MemoryMXBean memMXBean = ManagementFactory.getMemoryMXBean();

  private final QueryWrapper query;
  private final WorkManager workManager;
  private final WebUserConnection webUserConnection;
  private final SessionOptionManager options;

  public RestQueryRunner(final QueryWrapper query, final WorkManager workManager, final WebUserConnection webUserConnection) {
    this.query = query;
    this.workManager = workManager;
    this.webUserConnection = webUserConnection;
    this.options = webUserConnection.getSession().getOptions();
  }

  public QueryResult run() throws Exception {
    applyUserName();
    applyOptions();
    applyDefaultSchema();
    int maxRows = applyRowLimit();
    return submitQuery(maxRows);
  }

  private void applyUserName() {
    String userName = query.getUserName();
    if (!Strings.isNullOrEmpty(userName)) {
      DrillConfig config = workManager.getContext().getConfig();
      if (!config.getBoolean(ExecConstants.IMPERSONATION_ENABLED)) {
        throw UserException.permissionError()
          .message("User impersonation is not enabled")
          .build(logger);
      }
      InboundImpersonationManager inboundImpersonationManager = new InboundImpersonationManager();
      boolean isAdmin = !config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED) ||
        ImpersonationUtil.hasAdminPrivileges(
            webUserConnection.getSession().getCredentials().getUserName(),
            ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(options),
            ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(options));
      if (isAdmin) {
        // Admin user can impersonate any user they want to (when authentication is disabled, all users are admin)
        webUserConnection.getSession().replaceUserCredentials(
          inboundImpersonationManager,
          UserBitShared.UserCredentials.newBuilder().setUserName(userName).build());
      } else {
        // Check configured impersonation rules to see if this user is allowed to impersonate the given user
        inboundImpersonationManager.replaceUserOnSession(userName, webUserConnection.getSession());
      }
    }
  }

  private void applyOptions() {
    Map<String, String> options = query.getOptions();
    if (options != null) {
      SessionOptionManager sessionOptionManager = webUserConnection.getSession().getOptions();
      for (Map.Entry<String, String> entry : options.entrySet()) {
        sessionOptionManager.setLocalOption(entry.getKey(), entry.getValue());
      }
    }
  }

  private void applyDefaultSchema() throws ValidationException {
    String defaultSchema = query.getDefaultSchema();
    if (!Strings.isNullOrEmpty(defaultSchema)) {
      SessionOptionManager options = webUserConnection.getSession().getOptions();
      @SuppressWarnings("resource")
      SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(workManager.getContext());
      SchemaPlus rootSchema = schemaTreeProvider.createRootSchema(options);
      webUserConnection.getSession().setDefaultSchemaPath(defaultSchema, rootSchema);
    }
  }

  private int applyRowLimit() {
    int defaultMaxRows = webUserConnection.getSession().getOptions().getInt(ExecConstants.QUERY_MAX_ROWS);
    int maxRows;
    int limit = query.getAutoLimitRowCount();
    if (limit > 0 && defaultMaxRows > 0) {
      maxRows = Math.min(limit, defaultMaxRows);
    } else {
      maxRows = Math.max(limit, defaultMaxRows);
    }
    webUserConnection.setAutoLimitRowCount(maxRows);
    return maxRows;
  }

  public QueryResult submitQuery(int maxRows) {
    final RunQuery runQuery = RunQuery.newBuilder()
        .setType(QueryType.valueOf(query.getQueryType()))
        .setPlan(query.getQuery())
        .setResultsMode(QueryResultsMode.STREAM_FULL)
        .setAutolimitRowcount(maxRows)
        .build();

    // Heap usage threshold/trigger to provide resiliency on web server for queries submitted via HTTP
    double memoryFailureThreshold = workManager.getContext().getConfig().getDouble(ExecConstants.HTTP_MEMORY_HEAP_FAILURE_THRESHOLD);

    // Submit user query to Drillbit work queue.
    final QueryId queryId = workManager.getUserWorker().submitWork(webUserConnection, runQuery);

    boolean isComplete = false;
    boolean nearlyOutOfHeapSpace = false;
    float usagePercent = getHeapUsage();

    // Wait until the query execution is complete or there is error submitting the query
    logger.debug("Wait until the query execution is complete or there is error submitting the query");
    do {
      try {
        isComplete = webUserConnection.await(TimeUnit.SECONDS.toMillis(1)); //periodically timeout 1 sec to check heap
      } catch (InterruptedException e) {}
      usagePercent = getHeapUsage();
      if (memoryFailureThreshold > 0 && usagePercent > memoryFailureThreshold) {
        nearlyOutOfHeapSpace = true;
      }
    } while (!isComplete && !nearlyOutOfHeapSpace);

    // Fail if nearly out of heap space
    if (nearlyOutOfHeapSpace) {
      UserException almostOutOfHeapException = UserException.resourceError()
          .message("There is not enough heap memory to run this query using the web interface. ")
          .addContext("Please try a query with fewer columns or with a filter or limit condition to limit the data returned. ")
          .addContext("You can also try an ODBC/JDBC client. ")
          .build(logger);
      //Add event
      workManager.getBee().getForemanForQueryId(queryId)
        .addToEventQueue(QueryState.FAILED, almostOutOfHeapException);
      //Return NearlyOutOfHeap exception
      throw almostOutOfHeapException;
    }

    logger.trace("Query {} is completed ", queryId);

    if (webUserConnection.getError() != null) {
      throw new UserRemoteException(webUserConnection.getError());
    }

    // Return the QueryResult.
    return new QueryResult(queryId, webUserConnection, webUserConnection.results);
  }

  // Detect possible excess heap
  private float getHeapUsage() {
    return (float) memMXBean.getHeapMemoryUsage().getUsed() / memMXBean.getHeapMemoryUsage().getMax();
  }

  public static class QueryResult {
    private final String queryId;
    public final Collection<String> columns;
    public final List<Map<String, String>> rows;
    public final List<String> metadata;
    public final String queryState;
    public final int attemptedAutoLimit;

    // DRILL-6847:  Modified the constructor so that the method has access
    // to all the properties in webUserConnection
    public QueryResult(QueryId queryId, WebUserConnection webUserConnection, List<Map<String, String>> rows) {
      this.queryId = QueryIdHelper.getQueryId(queryId);
      this.columns = webUserConnection.columns;
      this.metadata = webUserConnection.metadata;
      this.queryState = webUserConnection.getQueryState();
      this.rows = rows;
      this.attemptedAutoLimit = webUserConnection.getAutoLimitRowCount();
    }

    public String getQueryId() {
      return queryId;
    }
  }
}