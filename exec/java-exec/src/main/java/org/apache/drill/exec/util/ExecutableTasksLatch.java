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

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.apache.drill.exec.testing.CountDownLatchInjection;
import org.apache.drill.exec.testing.NoOpControlsInjector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A wrapper class around {@linkplain ExecutorService an execution service} that allows a thread that instantiated an
 * instance of the class to wait for a submitted task to complete (either successfully or unsuccessfully) or to wait for
 * all submitted tasks to complete.
 * @param <C> type of tasks to execute. <tt>C</tt> must extend {@linkplain Callable}{@literal <Void>}
 */
public class ExecutableTasksLatch<C extends Callable<Void>> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecutableTasksLatch.class);

  /**
   * An interface that class {@literal <C>} may optionally implement to receive a callback when a submitted for execution
   * task starts or finishes.
   */
  public interface Notifiable {
    /**
     * Notify a task that it is assigned to a thread for execution
     */
    void started();

    /**
     * A callback in case a task is considered to be successfully completed (not cancelled).
     */
    void finished();
  }

  private final ExecutorService executor;
  private final Thread thread;
  private final AtomicInteger count;
  private final Queue<ExecutableTask<C>> executableTasks;
  private final CountDownLatchInjection testCountDownLatch;

  /**
   * Constructs an instance of <tt>ExecutableTasksLatch</tt>. A thread where construction is done becomes the waiting
   * thread.
   * @param executor instance of {@linkplain ExecutorService execution service} to wrap
   * @param testCountDownLatch optional {@linkplain CountDownLatchInjection}
   */
  public ExecutableTasksLatch(ExecutorService executor, CountDownLatchInjection testCountDownLatch) {
    this.executor = executor;
    thread = Thread.currentThread();
    count = new AtomicInteger();
    executableTasks = new LinkedList<>();
    this.testCountDownLatch = testCountDownLatch == null ? NoOpControlsInjector.LATCH : testCountDownLatch;
  }

  /**
   * Waits for an earliest submitted task to complete and removes task from a collection of known tasks.
   * @throws ExecutionException if task threw an Exception during execution
   * @throws InterruptedException if wait is interrupted
   */
  public void take() throws ExecutionException, InterruptedException {
    final ExecutableTask<C> task = executableTasks.peek();
    Preconditions.checkState(task != null, "No tasks are scheduled for execution");
    while (ExecutableTask.State.COMPLETING.compareTo(task.state.get()) > 0) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      LockSupport.park();
    }
    executableTasks.remove();
    while (ExecutableTask.State.COMPLETING.compareTo(task.state.get()) == 0) {
      Thread.yield();
    }

    if (task.exception != null) {
      throw task.exception;
    }
  }

  /**
   * @return immutable collection of submitted for execution tasks that are not yet taken from
   * the {@linkplain ExecutableTasksLatch}
   */
  public Collection<ExecutableTask<C>> getExecutableTasks() {
    return ImmutableList.copyOf(executableTasks);
  }

  /**
   * submits a task for execution by {@linkplain ExecutorService}
   * @param callable task to execute
   * @return newly created instance of {@linkplain ExecutableTask}
   */
  public ExecutableTask<C> execute(C callable) {
    ExecutableTasksLatch.ExecutableTask<C> task = new ExecutableTasksLatch.ExecutableTask<>(callable, this);
    executor.execute(task);
    count.getAndIncrement();
    if (!executableTasks.add(task)) {
      task.cancel(true);
      throw new IllegalStateException();
    }
    return task;
  }

  /**
   * Wait for completion of all tasks (cancelling them if cancel returns true)
   * @param cancel BooleanSupplier that should return <tt>true</tt> when tasks cancellation is requested
   */
  public void await(BooleanSupplier cancel) {
    Preconditions.checkState(Thread.currentThread() == thread,
        String.format("%s can be awaited only from the %s thread", getClass().getSimpleName(), thread.getName()));
    boolean cancelled = false;
    while (count.get() > 0) {
      if (!cancelled && cancel.getAsBoolean()) {
        executableTasks.forEach(task -> task.cancel(true));
        cancelled = true;
      } else {
        LockSupport.park();
      }
    }
  }

  /**
   * Helper class to wrap {@linkplain Callable}{@literal <Void>} with cancellation and waiting for completion support
   *
   */
  public static final class ExecutableTask<C extends Callable<Void>> implements Runnable {

    private enum State {
      NEW,
      COMPLETING,
      NORMAL,
      EXCEPTIONAL,
      CANCELLED,
      INTERRUPTING,
      INTERRUPTED
    }

    private final AtomicReference<State> state;
    private final AtomicReference<Thread> runner;
    private final C callable;
    private final ExecutableTasksLatch executableTasksLatch;

    private volatile ExecutionException exception;

    private ExecutableTask(C callable, ExecutableTasksLatch executableTasksLatch) {
      state = new AtomicReference<>(State.NEW);
      runner = new AtomicReference<>();
      this.callable = callable;
      this.executableTasksLatch = executableTasksLatch;
    }

    public C getCallable() {
      return callable;
    }

    @Override
    public void run() {
      final Thread thread = Thread.currentThread();
      if (runner.compareAndSet(null, thread)) {
        final String name = thread.getName();
        thread.setName(String.format("%s-%s-%d", callable.getClass().getSimpleName(), executableTasksLatch.thread.getName(), thread.getId()));
        if (callable instanceof Notifiable) {
          ((Notifiable)callable).started();
        }
        ExecutionException executionException = null;
        try {
          // Test only - Pause until interrupted by fragment thread
          executableTasksLatch.testCountDownLatch.await();
          if (state.get() == State.NEW) {
            callable.call();
          }
        } catch (InterruptedException e) {
          if (state.compareAndSet(State.NEW, State.INTERRUPTED)) {
            logger.warn("{} interrupted during the run", callable, e);
          }
        } catch (Throwable t) {
          executionException = new ExecutionException(t);
        } finally {
          if (state.compareAndSet(State.NEW, State.COMPLETING)) {
            if (executionException == null) {
              if (callable instanceof Notifiable) {
                ((Notifiable)callable).finished();
              }
              state.lazySet(State.NORMAL);
            } else {
              exception = executionException;
              state.lazySet(State.EXCEPTIONAL);
            }
          }
          executableTasksLatch.count.getAndDecrement();
          LockSupport.unpark(executableTasksLatch.thread);
          thread.setName(name);
          while (state.get() == State.INTERRUPTING) {
            Thread.yield();
          }
          // Clear interrupt flag
          Thread.interrupted();
        }
      }
    }

    void cancel(boolean mayInterruptIfRunning) {
      final Thread thread = Thread.currentThread();
      if (runner.compareAndSet(null, thread)) {
        executableTasksLatch.count.getAndDecrement();
        if (executableTasksLatch.executor instanceof ThreadPoolExecutor) {
          ((ThreadPoolExecutor)executableTasksLatch.executor).remove(this);
        }
      } else {
        if (mayInterruptIfRunning) {
          if (state.compareAndSet(State.NEW, State.INTERRUPTING)) {
            try {
              runner.get().interrupt();
            } finally {
              state.lazySet(State.INTERRUPTED);
            }
          }
        } else {
          state.compareAndSet(State.NEW, State.CANCELLED);
        }
      }
    }

    public ExecutionException getException() {
      return State.EXCEPTIONAL.compareTo(state.get()) == 0 ? exception : null;
    }
  }
}
