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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

/**
 * Class used to allow parallel executions of tasks in a simplified way. Also maintains and reports timings of task completion.
 * TODO: look at switching to fork join.
 * @param <V> The time value that will be returned when the task is executed.
 */
public abstract class TimedRunnable<V> implements Runnable {

  private static long TIMEOUT_PER_RUNNABLE_IN_MSECS = 15000;

  private volatile Exception e;
  private volatile long threadStart;
  private volatile long timeNanos;
  private volatile V value;

  @Override
  public final void run() {
    long start = System.nanoTime();
    threadStart=start;
    try{
      value = runInner();
    }catch(Exception e){
      this.e = e;
    }finally{
      timeNanos = System.nanoTime() - start;
    }
  }

  protected abstract V runInner() throws Exception ;
  protected abstract IOException convertToIOException(Exception e);

  public long getThreadStart(){
    return threadStart;
  }
  public long getTimeSpentNanos(){
    return timeNanos;
  }

  public final V getValue() throws IOException {
    if(e != null){
      if(e instanceof IOException){
        throw (IOException) e;
      }else{
        throw convertToIOException(e);
      }
    }

    return value;
  }

  private static class LatchedRunnable implements Runnable {
    final CountDownLatch latch;
    final Runnable runnable;

    public LatchedRunnable(CountDownLatch latch, Runnable runnable){
      this.latch = latch;
      this.runnable = runnable;
    }

    @Override
    public void run() {
      try{
        runnable.run();
      }finally{
        latch.countDown();
      }
    }
  }

  /**
   * Execute the list of runnables with the given parallelization.  At end, return values and report completion time
   * stats to provided logger. Each runnable is allowed a certain timeout. If the timeout exceeds, existing/pending
   * tasks will be cancelled and a {@link UserException} is thrown.
   * @param activity Name of activity for reporting in logger.
   * @param logger The logger to use to report results.
   * @param runnables List of runnables that should be executed and timed.  If this list has one item, task will be
   *                  completed in-thread. Runnable must handle {@link InterruptedException}s.
   * @param parallelism  The number of threads that should be run to complete this task.
   * @return The list of outcome objects.
   * @throws IOException All exceptions are coerced to IOException since this was build for storage system tasks initially.
   */
  public static <V> List<V> run(final String activity, final Logger logger, final List<TimedRunnable<V>> runnables, int parallelism) throws IOException {
    Stopwatch watch = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    long timedRunnableStart=System.nanoTime();
    if(runnables.size() == 1){
      parallelism = 1;
      runnables.get(0).run();
    }else{
      parallelism = Math.min(parallelism,  runnables.size());
      final ExtendedLatch latch = new ExtendedLatch(runnables.size());
      final ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);
      try{
        for(TimedRunnable<V> runnable : runnables){
          threadPool.submit(new LatchedRunnable(latch, runnable));
        }

        final long timeout = (long)Math.ceil((TIMEOUT_PER_RUNNABLE_IN_MSECS * runnables.size())/parallelism);
        if (!latch.awaitUninterruptibly(timeout)) {
          // Issue a shutdown request. This will cause existing threads to interrupt and pending threads to cancel.
          // It is highly important that the task Runnables are handling interrupts correctly.
          threadPool.shutdownNow();

          try {
            // Wait for 5s for currently running threads to terminate. Above call (threadPool.shutdownNow()) interrupts
            // any running threads. If the runnables are handling the interrupts properly they should be able to
            // wrap up and terminate. If not waiting for 5s here gives a chance to identify and log any potential
            // thread leaks.
            threadPool.awaitTermination(5, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for pending threads in activity '{}' to terminate.", activity);
          }

          final String errMsg = String.format("Waited for %dms, but tasks for '%s' are not complete. " +
              "Total runnable size %d, parallelism %d.", timeout, activity, runnables.size(), parallelism);
          logger.error(errMsg);
          throw UserException.resourceError()
              .message(errMsg)
              .build(logger);
        }
      } finally {
        if (!threadPool.isShutdown()) {
          threadPool.shutdown();
        }
      }
    }

    List<V> values = Lists.newArrayList();
    long sum = 0;
    long max = 0;
    long count = 0;
    // measure thread creation times
    long earliestStart=Long.MAX_VALUE;
    long latestStart=0;
    long totalStart=0;
    IOException excep = null;
    for(final TimedRunnable<V> reader : runnables){
      try{
        values.add(reader.getValue());
        sum += reader.getTimeSpentNanos();
        count++;
        max = Math.max(max, reader.getTimeSpentNanos());
        earliestStart=Math.min(earliestStart, reader.getThreadStart() - timedRunnableStart);
        latestStart=Math.max(latestStart, reader.getThreadStart()-timedRunnableStart);
        totalStart+=latestStart=Math.max(latestStart, reader.getThreadStart()-timedRunnableStart);
      }catch(IOException e){
        if(excep == null){
          excep = e;
        }else{
          excep.addSuppressed(e);
        }
      }
    }

    if (watch != null) {
      double avg = (sum/1000.0/1000.0)/(count*1.0d);
      double avgStart = (totalStart/1000.0)/(count*1.0d);

      logger.debug(
          String.format("%s: Executed %d out of %d using %d threads. "
              + "Time: %dms total, %fms avg, %dms max.",
              activity, count, runnables.size(), parallelism, watch.elapsed(TimeUnit.MILLISECONDS), avg, max/1000/1000));
      logger.debug(
              String.format("%s: Executed %d out of %d using %d threads. "
                              + "Earliest start: %f \u03BCs, Latest start: %f \u03BCs, Average start: %f \u03BCs .",
                      activity, count, runnables.size(), parallelism, earliestStart/1000.0, latestStart/1000.0, avgStart));
      watch.stop();
    }

    if (excep != null) {
      throw excep;
    }

    return values;

  }
}
