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
package org.apache.drill.test;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;

import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.common.util.TestTools;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DrillTest {
  static final Logger logger = org.slf4j.LoggerFactory.getLogger(DrillTest.class);

  protected static final ObjectMapper objectMapper;
  static {
    System.setProperty("line.separator", "\n");
    objectMapper = new ObjectMapper();
  }

  static final SystemManager manager = new SystemManager();

  static final Logger testReporter = org.slf4j.LoggerFactory.getLogger("org.apache.drill.TestReporter");
  static final TestLogReporter LOG_OUTCOME = new TestLogReporter();

  static MemWatcher memWatcher;
  static String className;

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(50000);
  @Rule public final TestLogReporter logOutcome = LOG_OUTCOME;

  @Rule public TestName TEST_NAME = new TestName();

  @Before
  public void printID() throws Exception {
    System.out.printf("Running %s#%s\n", getClass().getName(), TEST_NAME.getMethodName());
  }

  @BeforeClass
  public static void initDrillTest() throws Exception {
    memWatcher = new MemWatcher();
  }

  @AfterClass
  public static void finiDrillTest() throws InterruptedException{
    testReporter.info(String.format("Test Class done (%s): %s.", memWatcher.getMemString(true), className));
    LOG_OUTCOME.sleepIfFailure();
  }

  protected static class MemWatcher {
    private long startDirect;
    private long startHeap;
    private long startNonHeap;

    public MemWatcher() {
      startDirect = manager.getMemDirect();
      startHeap = manager.getMemHeap();
      startNonHeap = manager.getMemNonHeap();
    }

    public Object getMemString() {
      return getMemString(false);
    }

    public String getMemString(boolean runGC) {
      if (runGC) {
        Runtime.getRuntime().gc();
      }
      long endDirect = manager.getMemDirect();
      long endHeap = manager.getMemHeap();
      long endNonHeap = manager.getMemNonHeap();
      return String.format("d: %s(%s), h: %s(%s), nh: %s(%s)", //
          DrillStringUtils.readable(endDirect - startDirect), DrillStringUtils.readable(endDirect), //
          DrillStringUtils.readable(endHeap - startHeap), DrillStringUtils.readable(endHeap), //
          DrillStringUtils.readable(endNonHeap - startNonHeap), DrillStringUtils.readable(endNonHeap) //
       );
    }

  }

  private static class TestLogReporter extends TestWatcher {

    private MemWatcher memWatcher;
    private int failureCount = 0;

    @Override
    protected void starting(Description description) {
      super.starting(description);
      className = description.getClassName();
      memWatcher = new MemWatcher();
    }

    @Override
    protected void failed(Throwable e, Description description) {
      testReporter.error(String.format("Test Failed (%s): %s", memWatcher.getMemString(), description.getDisplayName()), e);
      failureCount++;
    }

    @Override
    public void succeeded(Description description) {
      testReporter.info(String.format("Test Succeeded (%s): %s", memWatcher.getMemString(), description.getDisplayName()));
    }

    public void sleepIfFailure() throws InterruptedException {
      if(failureCount > 0){
        Thread.sleep(2000);
        failureCount = 0;
      } else {
        // pause to get logger to catch up.
        Thread.sleep(250);
      }
    }

  }

  public static String escapeJsonString(String original) {
    try {
      return objectMapper.writeValueAsString(original);
    } catch (JsonProcessingException e) {
      return original;
    }
  }

  private static class SystemManager {

    final BufferPoolMXBean directBean;
    final MemoryMXBean memoryBean;

    public SystemManager(){
      memoryBean = ManagementFactory.getMemoryMXBean();
      BufferPoolMXBean localBean = null;
      List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
      for(BufferPoolMXBean b : pools){
        if(b.getName().equals("direct")){
          localBean = b;

        }
      }
      directBean = localBean;
    }

    public long getMemDirect() {
      return directBean.getMemoryUsed();
    }

    public long getMemHeap() {
      return memoryBean.getHeapMemoryUsage().getUsed();
    }

    public long getMemNonHeap() {
      return memoryBean.getNonHeapMemoryUsage().getUsed();
    }

  }

}
