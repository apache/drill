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
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.QueryResultsMode;
import org.apache.drill.exec.work.WorkManager;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@XmlRootElement
public class QueryWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWrapper.class);

  private final String query;

  private final String queryType;

  @JsonCreator
  public QueryWrapper(@JsonProperty("query") String query, @JsonProperty("queryType") String queryType) {
    this.query = query;
    this.queryType = queryType;
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

    // Wait until the query execution is complete or there is error submitting the query
    webUserConnection.await();

    if (logger.isTraceEnabled()) {
      logger.trace("Query {} is completed ", queryId);
    }

    if (webUserConnection.results.isEmpty()) {
      webUserConnection.results.add(Maps.<String, String>newHashMap());
    }

    // Return the QueryResult.
    return new QueryResult(webUserConnection.columns, webUserConnection.results);
  }

  public static class QueryResult {
    public final Collection<String> columns;

    public final List<Map<String, String>> rows;

    public QueryResult(Collection<String> columns, List<Map<String, String>> rows) {
      this.columns = columns;
      this.rows = rows;
    }
  }

  @Override
  public String toString() {
    return "QueryRequest [queryType=" + queryType + ", query=" + query + "]";
  }

}
