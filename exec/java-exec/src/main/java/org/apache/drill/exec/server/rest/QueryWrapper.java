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

package org.apache.drill.exec.server.rest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.vector.ValueVector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

@XmlRootElement
public class QueryWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWrapper.class);

  private String query;
  private String queryType;

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

  public UserBitShared.QueryType getType() {
    UserBitShared.QueryType type = UserBitShared.QueryType.SQL;
    switch (queryType) {
      case "SQL" : type = UserBitShared.QueryType.SQL; break;
      case "LOGICAL" : type = UserBitShared.QueryType.LOGICAL; break;
      case "PHYSICAL" : type = UserBitShared.QueryType.PHYSICAL; break;
    }
    return type;
  }

  public QueryResult run(final DrillClient client, final BufferAllocator allocator) throws Exception {
    Listener listener = new Listener(allocator);
    client.runQuery(getType(), query, listener);
    listener.waitForCompletion();
    if (listener.results.isEmpty()) {
      listener.results.add(Maps.<String, String>newHashMap());
    }

    final Map<String, String> first = listener.results.get(0);
    for (String columnName : listener.columns) {
      if (!first.containsKey(columnName)) {
        first.put(columnName, null);
      }
    }

    return new QueryResult(listener.columns, listener.results);
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


  private static class Listener implements UserResultsListener {
    private volatile UserException exception;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final BufferAllocator allocator;
    public final List<Map<String, String>> results = Lists.newArrayList();
    public final Set<String> columns = Sets.newLinkedHashSet();

    Listener(BufferAllocator allocator) {
      this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    }

    @Override
    public void submissionFailed(UserException ex) {
      exception = ex;
      logger.error("Query Failed", ex);
      latch.countDown();
    }

    @Override
    public void queryCompleted(QueryState state) {
      latch.countDown();
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      try {
        final int rows = result.getHeader().getRowCount();
        if (result.hasData()) {
          RecordBatchLoader loader = null;
          try {
            loader = new RecordBatchLoader(allocator);
            loader.load(result.getHeader().getDef(), result.getData());
            // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
            // SchemaChangeException, so check/clean catch clause below.
            for (int i = 0; i < loader.getSchema().getFieldCount(); ++i) {
              columns.add(loader.getSchema().getColumn(i).getPath());
            }
            for (int i = 0; i < rows; ++i) {
              final Map<String, String> record = Maps.newHashMap();
              for (VectorWrapper<?> vw : loader) {
                final String field = vw.getValueVector().getMetadata().getNamePart().getName();
                final ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
                final Object value = i < accessor.getValueCount() ? accessor.getObject(i) : null;
                final String display = value == null ? null : value.toString();
                record.put(field, display);
              }
              results.add(record);
            }
          } finally {
            if (loader != null) {
              loader.clear();
            }
          }
        }
      } catch (SchemaChangeException e) {
        throw new RuntimeException(e);
      } finally {
        result.release();
      }
    }

    @Override
    public void queryIdArrived(UserBitShared.QueryId queryId) {
    }

    public void waitForCompletion() throws Exception {
      latch.await();
      if (exception != null) {
        throw exception;
      }
    }
  }
}
