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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.flatten.FlattenRecordBatch;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.vector.ValueVector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@XmlRootElement
public class QueryWrapper {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWrapper.class);

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

  public List<Map<String, Object>> run(DrillConfig config, ClusterCoordinator coordinator, BufferAllocator allocator)
    throws Exception {
    try(DrillClient client = new DrillClient(config, coordinator, allocator)){
      Listener listener = new Listener(new RecordBatchLoader(allocator));

      client.connect();
      client.runQuery(getType(), query, listener);

      List<Map<String, Object>> result = listener.waitForCompletion();
      return result;
    }
  }

  @Override
  public String toString() {
    return "QueryRequest [queryType=" + queryType + ", query=" + query + "]";
  }


  private static class Listener implements UserResultsListener {
    private volatile Exception exception;
    private AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);
    private List<Map<String, Object>> output = new LinkedList<>();
    private ArrayList<String> columnNames;
    private RecordBatchLoader loader;
    private boolean schemaAdded = false;

    Listener(RecordBatchLoader loader) {
      this.loader = loader;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      exception = ex;
      logger.error("Query Failed", ex);
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
        try {
          loader.load(result.getHeader().getDef(), result.getData());
          if (!schemaAdded) {
            columnNames = new ArrayList<>();
            for (int i = 0; i < loader.getSchema().getFieldCount(); ++i) {
              columnNames.add(loader.getSchema().getColumn(i).getPath().getAsUnescapedPath());
            }
            schemaAdded = true;
          }
        } catch (SchemaChangeException e) {
          throw new RuntimeException(e);
        }
        for (int i = 0; i < rows; ++i) {
          Map<String, Object> record = new HashMap<>();
          int j = 0;
          for (VectorWrapper<?> vw : loader) {
            ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
            Object object = accessor.getObject(i);
            if (object != null && (! object.getClass().getName().startsWith("java.lang"))) {
              object = object.toString();
            }
            if (object != null) {
              record.put(columnNames.get(j), object);
            } else {
              record.put(columnNames.get(j), null);
            }
            ++j;
          }
          output.add(record);
        }
      }
      result.release();
      if (result.getHeader().getIsLastChunk()) {
        latch.countDown();
      }
    }

    @Override
    public void queryIdArrived(UserBitShared.QueryId queryId) {
    }

    public List<Map<String, Object>> waitForCompletion() throws Exception {
      latch.await();
      if (exception != null) {
        throw exception;
      }
      return output;
    }
  }
}
