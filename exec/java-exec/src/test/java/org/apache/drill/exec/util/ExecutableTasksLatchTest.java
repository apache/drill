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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ExecutableTasksLatchTest {
  Logger logger = LoggerFactory.getLogger(ExecutableTasksLatchTest.class);

  private ExecutableTasksLatch<Callable<Void>> executableTasksLatch;
  private ExecutorService executor;
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    executor = Executors.newSingleThreadExecutor();
    executableTasksLatch = new ExecutableTasksLatch<>(executor, null);
  }

  @After
  public void teardown() {
    executor.shutdown();
  }

  @Test
  public void execute() {
    ExecutableTasksLatch.ExecutableTask<Callable<Void>> task = executableTasksLatch.execute(() -> null);
    assertNotNull(task);
    assertNotNull(task.getCallable());
    assertNull(task.getException());
    // no-op: task should be already done
    task.cancel(true);
  }

  @Test
  public void take() throws ExecutionException, InterruptedException {
    executableTasksLatch.execute(() -> null);
    executableTasksLatch.take();
  }

  @Test
  public void takeEmpty() throws ExecutionException, InterruptedException {
    thrown.expect(IllegalStateException.class);
    executableTasksLatch.take();
  }

  @Test
  public void takeEmpty2() throws ExecutionException, InterruptedException {
    executableTasksLatch.execute(() -> null);
    executableTasksLatch.take();
    thrown.expect(IllegalStateException.class);
    executableTasksLatch.take();
  }

  @Test
  public void takeException() throws ExecutionException, InterruptedException {
    executableTasksLatch.execute(() -> {
      throw new Exception();
    });
    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(Exception.class));
    executableTasksLatch.take();
  }

  @Test
  public void getExecutableTasks() throws ExecutionException, InterruptedException {
    assertTrue(executableTasksLatch.getExecutableTasks().isEmpty());
    executableTasksLatch.execute(() -> null);
    assertFalse(executableTasksLatch.getExecutableTasks().isEmpty());
    executableTasksLatch.take();
    assertTrue(executableTasksLatch.getExecutableTasks().isEmpty());
  }

  @Test
  public void await() {
    Thread thread = Thread.currentThread();
    executableTasksLatch.execute(() -> {
      thread.join();
      return null;
    });
    executableTasksLatch.await(() -> true);
  }

  @Test
  public void getException() {
    Exception exception = new Exception();
    for (int i = 0; i < 100; i++) {
      executableTasksLatch.execute(() -> {
        throw exception;
      });
    }
    executableTasksLatch.await(() -> false);
    final Collection<ExecutableTasksLatch.ExecutableTask<Callable<Void>>> tasks = executableTasksLatch.getExecutableTasks();
    assertEquals(100, tasks.size());
    for (ExecutableTasksLatch.ExecutableTask<Callable<Void>> task : tasks) {
      assertNotNull(task.getException());
      assertSame(exception, task.getException().getCause());
    }
  }
}
