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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.proto.UserProtos.QueryResultsMode;
import org.apache.drill.exec.work.WorkManager;

import javax.xml.bind.annotation.XmlRootElement;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@XmlRootElement
public class QueryWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWrapper.class);

  private final String query;
  private final String queryType;
  private final int autoLimitRowCount;

  private static MemoryMXBean memMXBean = ManagementFactory.getMemoryMXBean();

  @JsonCreator
  public QueryWrapper(@JsonProperty("query") String query, @JsonProperty("queryType") String queryType, @JsonProperty("autoLimit") String autoLimit) {
    this.query = query;
    this.queryType = queryType.toUpperCase();
    this.autoLimitRowCount = autoLimit != null && autoLimit.matches("[0-9]+") ? Integer.valueOf(autoLimit) : 0;
  }

  public String getQuery() {
    return query;
  }

  public String getQueryType() {
    return queryType;
  }

  public QueryType getType() {
    return QueryType.valueOf(queryType);
  }

  public QueryResult run(final WorkManager workManager, final WebUserConnection webUserConnection) throws Exception {
    final RunQuery runQuery = RunQuery.newBuilder().setType(getType())
        .setPlan(getQuery())
        .setResultsMode(QueryResultsMode.STREAM_FULL)
        .setAutolimitRowcount(autoLimitRowCount)
        .build();

    int defaultMaxRows = webUserConnection.getSession().getOptions().getOption(ExecConstants.QUERY_MAX_ROWS).num_val.intValue();
    int maxRows;
    if (autoLimitRowCount > 0 && defaultMaxRows > 0) {
      maxRows = Math.min(autoLimitRowCount, defaultMaxRows);
    } else {
      maxRows = Math.max(autoLimitRowCount, defaultMaxRows);
    }
    webUserConnection.setAutoLimitRowCount(maxRows);

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

    //Fail if nearly out of heap space
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

  //Detect possible excess heap
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

    //DRILL-6847:  Modified the constructor so that the method has access to all the properties in webUserConnection
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

  @Override
  public String toString() {
    return "QueryRequest [queryType=" + queryType + ", query=" + query + "]";
  }

}
