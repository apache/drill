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

import io.netty.buffer.DrillBuf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.util.VectorUtil;

import com.google.common.base.Stopwatch;

public class PrintingResultsListener implements UserResultsListener {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PrintingResultsListener.class);
  private final AtomicInteger count = new AtomicInteger();
  private final CountDownLatch latch = new CountDownLatch(1);
  private RecordBatchLoader loader;
  private Format format;
  private final int    columnWidth;
  private final BufferAllocator allocator;
  private volatile UserException exception;
  private QueryId queryId;
  private final Stopwatch w = new Stopwatch();

  public PrintingResultsListener(DrillConfig config, Format format, int columnWidth) {
    this.allocator = RootAllocatorFactory.newRoot(config);
    loader = new RecordBatchLoader(allocator);
    this.format = format;
    this.columnWidth = columnWidth;
  }

  @Override
  public void submissionFailed(UserException ex) {
    exception = ex;
    System.out.println("Exception (no rows returned): " + ex + ".  Returned in " + w.elapsed(TimeUnit.MILLISECONDS)
        + "ms.");
    latch.countDown();
  }

  @Override
  public void queryCompleted(QueryState state) {
    DrillAutoCloseables.closeNoChecked(allocator);
    latch.countDown();
    System.out.println("Total rows returned : " + count.get() + ".  Returned in " + w.elapsed(TimeUnit.MILLISECONDS)
        + "ms.");
  }

  @Override
  public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
    final QueryData header = result.getHeader();
    final DrillBuf data = result.getData();

    if (data != null) {
      count.addAndGet(header.getRowCount());
      try {
        loader.load(header.getDef(), data);
        // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
        // SchemaChangeException, so check/clean catch clause below.
      } catch (SchemaChangeException e) {
        submissionFailed(UserException.systemError(e).build(logger));
      }

      switch(format) {
        case TABLE:
          VectorUtil.showVectorAccessibleContent(loader, columnWidth);
          break;
        case TSV:
          VectorUtil.showVectorAccessibleContent(loader, "\t");
          break;
        case CSV:
          VectorUtil.showVectorAccessibleContent(loader, ",");
          break;
      }
      loader.clear();
    }

    result.release();
  }

  public int await() throws Exception {
    latch.await();
    if (exception != null) {
      exception.addSuppressed(new DrillRuntimeException("Exception in executor threadpool"));
      throw exception;
    }
    return count.get();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public void queryIdArrived(QueryId queryId) {
    w.start();
    this.queryId = queryId;
  }
}
