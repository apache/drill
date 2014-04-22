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

import org.apache.drill.common.util.TestTools;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;

public class DrillTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillTest.class);

  static final SystemManager manager = new SystemManager();
  static final TestLogReporter LOG_OUTCOME = new TestLogReporter(org.slf4j.LoggerFactory.getLogger("org.apache.drill.TestReporter"));

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(50000);
  @Rule public final TestLogReporter logOutcome = LOG_OUTCOME;

  @AfterClass
  public static void letLogsCatchUp() throws InterruptedException{
    LOG_OUTCOME.sleepIfFailure();
  }

  private static class TestLogReporter extends TestWatcher{


    private int failureCount = 0;

    final Logger logger;
    private long startDirect;
    private long startHeap;
    private long startNonHeap;

    public TestLogReporter(Logger logger) {
      this.logger = logger;
    }


    @Override
    protected void starting(Description description) {
      startDirect = manager.getMemDirect();
      startHeap = manager.getMemHeap();
      startNonHeap = manager.getMemNonHeap();
      super.starting(description);
    }


    public String getMemString(){
      return String.format("d: %s, h: %s, nh: %s", //
          readable(manager.getMemDirect() - startDirect), //
          readable(manager.getMemHeap() - startHeap), //
          readable(manager.getMemNonHeap() - startNonHeap) //
          );
    }
    @Override
    protected void failed(Throwable e, Description description) {
      logger.error(String.format("Test Failed (%s): %s", getMemString(), description.getDisplayName()), e);
      failureCount++;
    }

    @Override
    public void succeeded(Description description) {
      logger.info(String.format("Test Succeeded (%s): %s", getMemString(), description.getDisplayName()));
    }

    public void sleepIfFailure() throws InterruptedException{
      if(failureCount > 0){
        Thread.sleep(2000);
        failureCount = 0;
      }
    }
  }

  public static String readable(long bytes) {
    int unit = 1024;
    if (bytes < unit) return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = ("KMGTPE").charAt(exp-1) + ("i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
}

  private static class SystemManager{

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

    public long getMemDirect(){
      return directBean.getMemoryUsed();
    }

    public long getMemHeap(){
      return memoryBean.getHeapMemoryUsage().getUsed();
    }

    public long getMemNonHeap(){
      return memoryBean.getNonHeapMemoryUsage().getUsed();
    }
  }}
