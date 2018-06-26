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
package org.apache.drill.exec.util;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.jetty.util.ConcurrentHashSet;

/** Utility class to enhance the Java {@link ExecutorService} class functionality */
public final class ExecutorServiceUtil {

  /**
   * Instantiate an {@link java.util.concurrent.ExecutorService} instance with the ability to block
   * when the {@link FutureTask#cancel(boolean)} method is called with the "mayInterruptIfRunning" parameter
   * set to true.
   *
   * @see {@link java.util.concurrent.Executors#newFixedThreadPool(int)}
   */
  public static ExecutorService newFixedThreadPool(int nThreads) {

    return new CustomThreadPoolExecutor(nThreads,
      nThreads,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<Runnable>());
  }

  /**
   * Instantiate an {@link java.util.concurrent.ExecutorService} instance with the ability to block
   * when the {@link FutureTask#cancel(boolean)} method is called with the "mayInterruptIfRunning" parameter
   * set to true.
   *
   * @see {@link java.util.concurrent.Executors#newFixedThreadPool(int, ThreadFactory)}
   */
  public static ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {

    return new CustomThreadPoolExecutor(nThreads,
      nThreads,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<Runnable>(),
      threadFactory);
  }


// ----------------------------------------------------------------------------
// Local Implementation
// ----------------------------------------------------------------------------

  /** Disabling object instantiation */
  private ExecutorServiceUtil() {
  }


// ----------------------------------------------------------------------------
// Inner classes
// ----------------------------------------------------------------------------

  /** Customizing the {@link ThreadPoolExecutor} class to expose a blocking version of task cancellation */
  private static final class CustomThreadPoolExecutor extends ThreadPoolExecutor {

    private CustomThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {

      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    private CustomThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {

      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }


    private CustomThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {

      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    private CustomThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {

      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      return new CustomFutureTask<T>(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
      return new CustomFutureTask<T>(callable);
    }
  }

  /**
   * Extends the {@link FutureTask#cancel(boolean)} method behavior by blocking the cancel call
   * when the "mayInterruptIfRunning" parameter is set.
   */
  private static final class CustomFutureTask<V> extends FutureTask<V> {
    // Captures task's execution state
    private enum STATE {
      NOT_RUNNING,
      RUNNING,
      DONE
    };

    /** Enables us to figure out the true running state of this task */
    private volatile STATE state                       = STATE.NOT_RUNNING;
    private final AtomicReference<Thread> runnerThread = new AtomicReference<>(null);
    private final Object monitor                       = new Object();
    private final Set<Thread> interruptingThreads      = new ConcurrentHashSet<>();


    private CustomFutureTask(Callable<V> callable) {
      super(callable);
    }

    private CustomFutureTask(Runnable runnable, V result) {
      super(runnable, result);
    }

    @Override
    public void run() {
      // Prevents concurrent invocation of the run method
      if (!runnerThread.compareAndSet(null, Thread.currentThread())) {
        return;
      }

      state = STATE.RUNNING;

      try {
        super.run();
      } finally {
        state = STATE.DONE;

        // Optimization: no need to notify if the state is not "cancelled"
        if (isCancelled()) {
          synchronized (monitor) {
            monitor.notifyAll();
          }
        }
        runnerThread.set(null);
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (mayInterruptIfRunning) {
        interruptingThreads.add(Thread.currentThread());
      }
      return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected void done() {

      // ALGORITHM - referring to the parent state unless otherwise noted
      //
      // - done() is called implies that the parent's state is different than NEW
      // - If the state is not cancelled, implies the task is done from running; note that cancellation is
      //   not possible anymore (can only happen if the state was NEW)
      // - Otherwise, the state is cancelled and only the cancel thread can invoke this method
      // - The cancel thread will check whether it needs to block waiting:
      //   a) In case it has requested interruption of the future task
      //   b) The task is still running
      // - If wait is granted, then it will block as long as the task is running

      // Note: the cancel thread interrupt flag is preserved

      if (!isCancelled()) {
        return; // NOOP
      }

      // At this point we know we are dealing with the cancel thread

      if (shouldWait()) {
        // Save the current interrupted flag and clear it to allow wait operations
        boolean interrupted = Thread.interrupted();

        try {
          synchronized (monitor) {
            while (isRunning()) {
              try {
                monitor.wait();
              } catch (InterruptedException e) {
                interrupted = true;
              }
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    private boolean shouldWait() {
      return isRunning()                                         // The task is currently executing
        && interruptingThreads.contains(Thread.currentThread()); // the cancel thread wishes to interrupt this task
    }

    private boolean isRunning() {
      return state == STATE.RUNNING;
    }

  }
}