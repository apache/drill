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
package org.apache.drill.exec.store.parquet;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;

public class TestParquetPhysicalPlan extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetPhysicalPlan.class);

  public String fileName = "parquet/parquet_scan_filter_union_screen_physical.json";

  @Test
  @Ignore
  public void testParseParquetPhysicalPlan() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try (Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL, Resources.toString(Resources.getResource(fileName),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      int count = 0;
      for (QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        count += b.getHeader().getRowCount();
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.print(vw.getValueVector().getField().getPath() + ": ");
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            if (o instanceof byte[]) {
              System.out.print(" [" + new String((byte[]) o) + "]");
            } else {
              System.out.print(" [" + vv.getAccessor().getObject(i) + "]");
            }
//            break;
          }
          System.out.println();
        }
        loader.clear();
        b.release();
      }
      client.close();
      System.out.println(String.format("Got %d total results", count));
    }
  }

  private class ParquetResultsListener implements UserResultsListener {
    AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void submissionFailed(UserException ex) {
      logger.error("submission failed", ex);
      latch.countDown();
    }

    @Override
    public void queryCompleted(QueryState state) {
      latch.countDown();
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      System.out.println(String.format("Result batch arrived. Number of records: %d", rows));
      count.addAndGet(rows);
      result.release();
    }

    public int await() throws Exception {
      latch.await();
      return count.get();
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
    }
  }

  @Test
  @Ignore
  public void testParseParquetPhysicalPlanRemote() throws Exception {
    DrillConfig config = DrillConfig.create();

    try(DrillClient client = new DrillClient(config);) {
      client.connect();
      ParquetResultsListener listener = new ParquetResultsListener();
      Stopwatch watch = Stopwatch.createStarted();
      client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL, Resources.toString(Resources.getResource(fileName),Charsets.UTF_8), listener);
      System.out.println(String.format("Got %d total records in %d seconds", listener.await(), watch.elapsed(TimeUnit.SECONDS)));
      client.close();
    }
  }

}
