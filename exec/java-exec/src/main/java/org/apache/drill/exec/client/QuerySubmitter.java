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
package org.apache.drill.exec.client;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;
import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RemoteRpcException;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QuerySubmitter {

  public static void main(String args[]) throws Exception {
    QuerySubmitter submitter = new QuerySubmitter();
    System.exit(submitter.submitQuery(args[0], args[1], args[2]));
  }

  public int submitQuery(String planLocation, String type, String zkQuorum) throws Exception {
    DrillConfig config = DrillConfig.create();
    DrillClient client;
    if (zkQuorum.equals("local")) {
      RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
      Drillbit bit = new Drillbit(config, serviceSet);
      bit.run();
      client = new DrillClient(config, serviceSet.getCoordinator());
    } else {
      ZKClusterCoordinator clusterCoordinator = new ZKClusterCoordinator(config, zkQuorum);
      clusterCoordinator.start(10000);
      client = new DrillClient(config, clusterCoordinator);
    }
    client.connect();
    QueryResultsListener listener = new QueryResultsListener();
    String plan = Charsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(planLocation)))).toString();
    UserProtos.QueryType queryType;
    type = type.toLowerCase();
    switch(type) {
      case "logical":
        queryType = UserProtos.QueryType.LOGICAL;
        break;
      case "physical":
        queryType = UserProtos.QueryType.PHYSICAL;
        break;
      default:
        System.out.println("Invalid query type: " + type);
        return -1;
    }
    Stopwatch watch = new Stopwatch();
    watch.start();
    client.runQuery(queryType, plan, listener);
    int rows = listener.await();
    System.out.println(String.format("Got %d record%s in %f seconds", rows, rows > 1 ? "s" : "", (float)watch.elapsed(TimeUnit.MILLISECONDS) / (float)1000));
    return 0;
  }

  private class QueryResultsListener implements UserResultsListener {
    AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);
    RecordBatchLoader loader = new RecordBatchLoader(new BootStrapContext(DrillConfig.create()).getAllocator());
    int width;

    @Override
    public void submissionFailed(RpcException ex) {
      System.out.println(String.format("Query failed: %s", ex));
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        try {
          loader.load(result.getHeader().getDef(), result.getData());
        } catch (SchemaChangeException e) {
          submissionFailed(new RpcException(e));
        }
        List<String> columns = Lists.newArrayList();
        for (VectorWrapper vw : loader) {
          columns.add(vw.getValueVector().getField().getName());
        }
        width = columns.size();
        for (int row = 0; row < rows; row++) {
          if (row%50 == 0) {
            System.out.println(StringUtils.repeat("-", width*17 + 1));
            for (String column : columns) {
              System.out.printf("| %-15s", width <= 15 ? column : column.substring(0, 14));
            }
            System.out.printf("|\n");
            System.out.println(StringUtils.repeat("-", width*17 + 1));
          }
          for (VectorWrapper vw : loader) {
            Object o = vw.getValueVector().getAccessor().getObject(row);
            if (o instanceof byte[]) {
              String value = new String((byte[]) o);
              count.addAndGet(1);
              System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0, 14));
            } else {
              String value = o.toString();
              count.addAndGet(1);
              System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0,14));
            }
          }
          System.out.printf("|\n");
        }
      }
      if (result.getHeader().getIsLastChunk()) {
        System.out.println(StringUtils.repeat("-", width*17 + 1));
        latch.countDown();
      }
      result.release();
    }

    public int await() throws Exception {
      latch.await();
      return count.get();
    }
  }
}
