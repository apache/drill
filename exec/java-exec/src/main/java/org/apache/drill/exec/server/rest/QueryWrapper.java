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
import com.google.common.collect.Maps;

import org.apache.drill.common.exceptions.UserException;
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

  private static MemoryMXBean memMXBean = ManagementFactory.getMemoryMXBean();
  private double heapMemoryFailureThreshold;

  @JsonCreator
  public QueryWrapper(@JsonProperty("query") String query, @JsonProperty("queryType") String queryType) {
    this.query = query;
    this.queryType = queryType.toUpperCase();
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
        .build();

    // Submit user query to Drillbit work queue.
    final QueryId queryId = workManager.getUserWorker().submitWork(webUserConnection, runQuery);

    heapMemoryFailureThreshold = workManager.getContext().getConfig().getDouble( ExecConstants.HTTP_QUERY_FAIL_LOW_HEAP_THRESHOLD );
    boolean isComplete = false;
    boolean nearlyOutOfHeapSpace = false;
    float usagePercent = getHeapUsage();

    // Wait until the query execution is complete or there is error submitting the query
    logger.debug("Wait until the query execution is complete or there is error submitting the query");
    do {
      try {
        isComplete = webUserConnection.await(TimeUnit.SECONDS.toMillis(1)); /*periodically timeout to check heap*/
      } catch (Exception e) { }

      usagePercent = getHeapUsage();
      if (usagePercent >  heapMemoryFailureThreshold) {
        nearlyOutOfHeapSpace = true;
      }
    } while (!isComplete && !nearlyOutOfHeapSpace);

    //Fail if nearly out of heap space
    if (nearlyOutOfHeapSpace) {
      workManager.getBee().getForemanForQueryId(queryId)
        .addToEventQueue(QueryState.FAILED,
            UserException.resourceError(
                new Throwable(
                    "Query submitted through the Web interface was failed due to diminishing free heap memory ("+ Math.floor(((1-usagePercent)*100)) +"% free). "
                        + "Limit the number of columns or rows returned in the query, or retry using an ODBC/JDBC client."
                    )
                )
              .build(logger)
            );
    }

    if (logger.isTraceEnabled()) {
      logger.trace("Query {} is completed ", queryId);
    }

    if (webUserConnection.results.isEmpty()) {
      webUserConnection.results.add(Maps.<String, String>newHashMap());
    }

    // Return the QueryResult.
    return new QueryResult(queryId, webUserConnection.columns, webUserConnection.results);
  }

  //Detect possible excess heap
  private float getHeapUsage() {
    return (float) memMXBean.getHeapMemoryUsage().getUsed() / memMXBean.getHeapMemoryUsage().getMax();
  }

  public static class QueryResult {
    private final String queryId;
    public final Collection<String> columns;
    public final List<Map<String, String>> rows;

    public QueryResult(QueryId queryId, Collection<String> columns, List<Map<String, String>> rows) {
      this.queryId = QueryIdHelper.getQueryId(queryId);
      this.columns = columns;
      this.rows = rows;
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
