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
package org.apache.drill.exec.physical.impl.partitionsender;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.testing.CountDownLatchInjection;
import org.apache.drill.exec.util.ExecutableTasksLatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Decorator class to hide multiple Partitioner existence from the caller
 * since this class involves multithreaded processing of incoming batches
 * as well as flushing it needs special handling of OperatorStats - stats
 * since stats are not suitable for use in multithreaded environment
 * The algorithm to figure out processing versus wait time is based on following formula:
 * totalWaitTime = totalAllPartitionersProcessingTime - max(sum(processingTime) by partitioner)
 */
public final class PartitionerDecorator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerDecorator.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(PartitionerDecorator.class);

  private List<Partitioner> partitioners;
  private final OperatorStats stats;
  private final FragmentContext context;
  private final boolean enableParallelTaskExecution;

  PartitionerDecorator(List<Partitioner> partitioners, OperatorStats stats, FragmentContext context) {
    this(partitioners, stats, context, partitioners.size() > 1);
  }

  PartitionerDecorator(List<Partitioner> partitioners, OperatorStats stats, FragmentContext context, boolean enableParallelTaskExecution) {
    this.partitioners = partitioners;
    this.stats = stats;
    this.context = context;
    this.enableParallelTaskExecution = enableParallelTaskExecution;
  }

  /**
   * partitionBatch - decorator method to call real Partitioner(s) to process incoming batch
   * uses either threading or not threading approach based on number Partitioners
   * @param incoming
   * @throws ExecutionException
   */
  public void partitionBatch(final RecordBatch incoming) throws ExecutionException {
    executeMethodLogic(new PartitionBatchHandlingClass(incoming));
  }

  /**
   * flushOutgoingBatches - decorator to call real Partitioner(s) flushOutgoingBatches
   * @param isLastBatch
   * @param schemaChanged
   * @throws ExecutionException
   */
  public void flushOutgoingBatches(final boolean isLastBatch, final boolean schemaChanged) throws ExecutionException {
    executeMethodLogic(new FlushBatchesHandlingClass(isLastBatch, schemaChanged));
  }

  /**
   * decorator method to call multiple Partitioners initialize()
   */
  public void initialize() {
    for (Partitioner part : partitioners) {
      part.initialize();
    }
  }

  /**
   * decorator method to call multiple Partitioners clear()
   */
  public void clear() {
    for (Partitioner part : partitioners) {
      part.clear();
    }
  }

  /**
   * Helper method to get PartitionOutgoingBatch based on the index
   * since we may have more then one Partitioner
   * As number of Partitioners should be very small AND this method it used very rarely,
   * so it is OK to loop in order to find right partitioner
   * @param index - index of PartitionOutgoingBatch
   * @return PartitionOutgoingBatch
   */
  public PartitionOutgoingBatch getOutgoingBatches(int index) {
    for (Partitioner part : partitioners) {
      PartitionOutgoingBatch outBatch = part.getOutgoingBatch(index);
      if (outBatch != null) {
        return outBatch;
      }
    }
    return null;
  }

  List<Partitioner> getPartitioners() {
    return partitioners;
  }

  /**
   * Helper to execute the different methods wrapped into same logic
   * @param iface
   * @throws ExecutionException
   */
  @VisibleForTesting
  void executeMethodLogic(final GeneralExecuteIface iface) throws ExecutionException {
    // To simulate interruption of main fragment thread and interrupting the partition threads, create a
    // CountDownInject latch. Partitioner threads await on the latch and main fragment thread counts down or
    // interrupts waiting threads. This makes sure that we are actually interrupting the blocked partitioner threads.
    try (CountDownLatchInjection testCountDownLatch = injector.getLatch(context.getExecutionControls(), "partitioner-sender-latch")) {
      testCountDownLatch.initialize(1);
      final ExecutorService executor = enableParallelTaskExecution ? context.getExecutor() : MoreExecutors.newDirectExecutorService();
      ExecutableTasksLatch<PartitionerTask> executableTasksLatch = new ExecutableTasksLatch<>(executor, testCountDownLatch);
      ExecutionException executionException = null;
      // start waiting on main stats to adjust by sum(max(processing)) at the end
      startWait();
      try {
        partitioners.forEach(partitioner -> executableTasksLatch.execute(new PartitionerTask(iface, partitioner)));
        // Wait for main fragment interruption.
        injector.injectInterruptiblePause(context.getExecutionControls(), "wait-for-fragment-interrupt", logger);
        testCountDownLatch.countDown();
      } catch (InterruptedException e) {
        logger.warn("fragment thread interrupted", e);
      } catch (RejectedExecutionException e) {
        logger.warn("Failed to execute partitioner tasks. Execution service down?", e);
        executionException = new ExecutionException(e);
      } finally {
        executableTasksLatch.await(() -> {
          boolean cancel = !context.getExecutorState().shouldContinue();
          if (cancel) {
            logger.warn("Cancelling fragment {} tasks...", context.getFragIdString());
          } else {
            logger.debug("Waiting for fragment {} tasks to complete...", context.getFragIdString());
          }
          return cancel;
        });
        stopWait();
        processPartitionerTasks(executableTasksLatch.getExecutableTasks(), executionException);
      }
    }
  }

  private void startWait() {
    if (enableParallelTaskExecution) {
      stats.startWait();
    }
  }

  private void stopWait() {
    if (enableParallelTaskExecution) {
      stats.stopWait();
    }
  }

  private void processPartitionerTasks(Collection<ExecutableTasksLatch.ExecutableTask<PartitionerTask>> executableTasks, ExecutionException executionException) throws ExecutionException {
    long maxProcessTime = 0L;
    for (ExecutableTasksLatch.ExecutableTask<PartitionerTask> executableTask : executableTasks) {
      ExecutionException e = executableTask.getException();
      if (e != null) {
        if (executionException == null) {
          executionException = e;
        } else {
          executionException.getCause().addSuppressed(e.getCause());
        }
      }
      if (executionException == null) {
        final OperatorStats localStats = executableTask.getCallable().getStats();
        // find out max Partitioner processing time
        if (enableParallelTaskExecution) {
          long currentProcessingNanos = localStats.getProcessingNanos();
          maxProcessTime = (currentProcessingNanos > maxProcessTime) ? currentProcessingNanos : maxProcessTime;
        } else {
          maxProcessTime += localStats.getWaitNanos();
        }
        stats.mergeMetrics(localStats);
      }
    }
    if (executionException != null) {
      throw executionException;
    }
    // scale down main stats wait time based on calculated processing time
    // since we did not wait for whole duration of above execution
    if (enableParallelTaskExecution) {
      stats.adjustWaitNanos(-maxProcessTime);
    } else {
      stats.adjustWaitNanos(maxProcessTime);
    }
  }

  /**
   * Helper interface to generalize functionality executed in the thread
   * since it is absolutely the same for partitionBatch and flushOutgoingBatches
   * protected is for testing purposes
   */
  protected interface GeneralExecuteIface {
    void execute(Partitioner partitioner) throws IOException;
  }

  /**
   * Class to handle running partitionBatch method
   *
   */
  private static class PartitionBatchHandlingClass implements GeneralExecuteIface {

    private final RecordBatch incoming;

    PartitionBatchHandlingClass(RecordBatch incoming) {
      this.incoming = incoming;
    }

    @Override
    public void execute(Partitioner part) throws IOException {
      part.partitionBatch(incoming);
    }
  }

  /**
   * Class to handle running flushOutgoingBatches method
   *
   */
  private static class FlushBatchesHandlingClass implements GeneralExecuteIface {

    private final boolean isLastBatch;
    private final boolean schemaChanged;

    FlushBatchesHandlingClass(boolean isLastBatch, boolean schemaChanged) {
      this.isLastBatch = isLastBatch;
      this.schemaChanged = schemaChanged;
    }

    @Override
    public void execute(Partitioner part) throws IOException {
      part.flushOutgoingBatches(isLastBatch, schemaChanged);
    }
  }

  /**
   * Helper class to wrap Runnable with cancellation and waiting for completion support
   *
   */
  private static class PartitionerTask implements Callable<Void>, ExecutableTasksLatch.Notifiable {

    private final GeneralExecuteIface iface;
    private final Partitioner partitioner;

    PartitionerTask(GeneralExecuteIface iface, Partitioner partitioner) {
      this.iface = iface;
      this.partitioner = partitioner;
    }

    @Override
    public Void call() throws Exception {
      iface.execute(partitioner);
      return null;
    }

    @Override
    public void started() {
      final OperatorStats localStats = getStats();
      localStats.clear();
      localStats.startProcessing();
    }

    @Override
    public void finished() {
      getStats().stopProcessing();
    }

    public OperatorStats getStats() {
      return partitioner.getStats();
    }
  }
}
