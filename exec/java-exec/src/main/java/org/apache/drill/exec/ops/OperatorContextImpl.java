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
package org.apache.drill.exec.ops;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.work.WorkManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

class OperatorContextImpl extends AbstractOperatorExecContext implements OperatorContext, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorContextImpl.class);

  private boolean closed = false;
  private final OperatorStats stats;
  private DrillFileSystem fs;
  private final ExecutorService executor;
  private final ExecutorService scanExecutor;
  private final ExecutorService scanDecodeExecutor;

  /**
   * This lazily initialized executor service is used to submit a {@link Callable task} that needs a proxy user. There
   * is no pool that is created; this pool is a decorator around {@link WorkManager#executor the worker pool} that
   * returns a {@link ListenableFuture future} for every task that is submitted. For the shutdown sequence,
   * see {@link WorkManager#close}.
   */
  private ListeningExecutorService delegatePool;

  public OperatorContextImpl(PhysicalOperator popConfig, FragmentContext context) throws OutOfMemoryException {
    this(popConfig, context, null);
  }

  public OperatorContextImpl(PhysicalOperator popConfig, FragmentContext context, OperatorStats stats)
      throws OutOfMemoryException {
    super(context.getNewChildAllocator(popConfig.getClass().getSimpleName(),
          popConfig.getOperatorId(), popConfig.getInitialAllocation(), popConfig.getMaxAllocation()),
          popConfig, context.getExecutionControls(), stats);
    if (stats != null) {
      this.stats = stats;
    } else {
      OpProfileDef def =
          new OpProfileDef(popConfig.getOperatorId(), popConfig.getOperatorType(),
                           OperatorUtilities.getChildCount(popConfig));
      this.stats = context.getStats().newOperatorStats(def, allocator);
    }
    executor = context.getDrillbitContext().getExecutor();
    scanExecutor = context.getDrillbitContext().getScanExecutor();
    scanDecodeExecutor = context.getDrillbitContext().getScanDecodeExecutor();
  }

  // Allow an operator to use the thread pool
  @Override
  public ExecutorService getExecutor() {
    return executor;
  }

  @Override
  public ExecutorService getScanExecutor() {
    return scanExecutor;
  }

  @Override
  public ExecutorService getScanDecodeExecutor() {
    return scanDecodeExecutor;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      logger.debug("Attempted to close Operator context for {}, but context is already closed", popConfig != null ? popConfig.getClass().getName() : null);
      return;
    }
    logger.debug("Closing context for {}", popConfig != null ? popConfig.getClass().getName() : null);

    closed = true;
    try {
      super.close();
    } finally {
      if (fs != null) {
        try {
          fs.close();
          fs = null;
        } catch (IOException e) {
          throw new DrillRuntimeException(e);
        }
      }
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  @Override
  public <RESULT> ListenableFuture<RESULT> runCallableAs(final UserGroupInformation proxyUgi,
                                                         final Callable<RESULT> callable) {
    synchronized (this) {
      if (delegatePool == null) {
        delegatePool = MoreExecutors.listeningDecorator(executor);
      }
    }
    return delegatePool.submit(new Callable<RESULT>() {
      @Override
      public RESULT call() throws Exception {
        final Thread currentThread = Thread.currentThread();
        final String originalThreadName = currentThread.getName();
        currentThread.setName(proxyUgi.getUserName() + ":task-delegate-thread");
        final RESULT result;
        try {
          result = proxyUgi.doAs(new PrivilegedExceptionAction<RESULT>() {
            @Override
            public RESULT run() throws Exception {
              return callable.call();
            }
          });
        } finally {
          currentThread.setName(originalThreadName);
        }
        return result;
      }
    });
  }

  @Override
  public DrillFileSystem newFileSystem(Configuration conf) throws IOException {
    Preconditions.checkState(fs == null, "Tried to create a second FileSystem. Can only be called once per OperatorContext");
    fs = new DrillFileSystem(conf, getStats());
    return fs;
  }

  /**
   * Creates a DrillFileSystem that does not automatically track operator stats.
   */
  @Override
  public DrillFileSystem newNonTrackingFileSystem(Configuration conf) throws IOException {
    Preconditions.checkState(fs == null, "Tried to create a second FileSystem. Can only be called once per OperatorContext");
    fs = new DrillFileSystem(conf, null);
    return fs;
  }
}
