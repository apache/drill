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
package org.apache.drill.exec.testing;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.rpc.user.UserSession;
import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPauseInjection extends BaseTestQuery {

  private static final UserSession session = UserSession.Builder.newBuilder()
    .withOptionManager(bits[0].getContext().getOptionManager())
    .build();

  /**
   * Class whose methods we want to simulate pauses at run-time for testing
   * purposes. The class must have access to {@link org.apache.drill.exec.ops.QueryContext} or
   * {@link org.apache.drill.exec.ops.FragmentContext}.
   */
  private static class DummyClass {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(DummyClass.class);
    private static final ExecutionControlsInjector injector = ExecutionControlsInjector.getInjector(DummyClass.class);

    private final QueryContext context;

    public DummyClass(final QueryContext context) {
      this.context = context;
    }

    public static final String PAUSES = "<<pauses>>";

    /**
     * Method that pauses.
     *
     * @return how long the method paused in milliseconds
     */
    public long pauses() {
      // ... code ...

      final long startTime = System.currentTimeMillis();
      // simulated pause
      injector.injectPause(context.getExecutionControls(), PAUSES, logger);
      final long endTime = System.currentTimeMillis();

      // ... code ...
      return (endTime - startTime);
    }
  }

  @Test
  public void pauseInjected() {
    final long pauseMillis = 1000L;
    final String jsonString = "{\"injections\":[{"
      + "\"type\":\"pause\"," +
      "\"siteClass\":\"org.apache.drill.exec.testing.TestPauseInjection$DummyClass\","
      + "\"desc\":\"" + DummyClass.PAUSES + "\","
      + "\"millis\":" + pauseMillis + ","
      + "\"nSkip\":0,"
      + "\"nFire\":1"
      + "}]}";

    ControlsInjectionUtil.setControls(session, jsonString);

    final QueryContext queryContext = new QueryContext(session, bits[0].getContext());

    // test that the pause happens
    final DummyClass dummyClass = new DummyClass(queryContext);
    final long time = dummyClass.pauses();
    assertTrue((time >= pauseMillis));
    try {
      queryContext.close();
    } catch (Exception e) {
      fail();
    }
  }
}
