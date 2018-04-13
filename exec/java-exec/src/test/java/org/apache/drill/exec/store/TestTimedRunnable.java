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

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.test.TestTools;
import org.apache.drill.test.DrillTest;
import org.apache.drill.categories.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Unit testing for {@link TimedRunnable}.
 */
@Category({SlowTest.class})
public class TestTimedRunnable extends DrillTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTimedRunnable.class);

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(180000); // 3mins

  private static class TestTask extends TimedRunnable {
    final long sleepTime; // sleep time in ms

    public TestTask(final long sleepTime) {
      this.sleepTime = sleepTime;
    }

    @Override
    protected Void runInner() throws Exception {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        throw e;
      }
      return null;
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return new IOException("Failure while trying to sleep for sometime", e);
    }
  }

  @Test
  public void withoutAnyTasksTriggeringTimeout() throws Exception {
    List<TimedRunnable<TestTask>> tasks = Lists.newArrayList();

    for(int i=0; i<100; i++){
      tasks.add(new TestTask(2000));
    }

    TimedRunnable.run("Execution without triggering timeout", logger, tasks, 16);
  }

  @Test
  public void withTasksExceedingTimeout() throws Exception {
    UserException ex = null;

    try {
      List<TimedRunnable<TestTask>> tasks = Lists.newArrayList();

      for (int i = 0; i < 100; i++) {
        if ((i & (i + 1)) == 0) {
          tasks.add(new TestTask(2000));
        } else {
          tasks.add(new TestTask(20000));
        }
      }

      TimedRunnable.run("Execution with some tasks triggering timeout", logger, tasks, 16);
    } catch (UserException e) {
      ex = e;
    }

    assertNotNull("Expected a UserException", ex);
    assertThat(ex.getMessage(),
        containsString("Waited for 93750ms, but tasks for 'Execution with some tasks triggering timeout' are not " +
            "complete. Total runnable size 100, parallelism 16."));
  }

  @Test
  public void withManyTasks() throws Exception {

    List<TimedRunnable<TestTask>> tasks = Lists.newArrayList();

    for (int i = 0; i < 150000; i++) {
      tasks.add(new TestTask(0));
    }

    TimedRunnable.run("Execution with lots of tasks", logger, tasks, 16);
  }
}
