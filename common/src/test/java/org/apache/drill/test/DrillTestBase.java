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
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.common.util.TestTools;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract base class for all Drill tests.
 *
 * In addition to setting up and tearing down basic test infrastructure, this class also
 * provides randomization utilities for those classes which choose to avail themselves of it.
 *
 * @see "http://labs.carrotsearch.com/randomizedtesting.html"
 */
@Listeners(ReproducibleListener.class)
@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class DrillTestBase {

  private static final Logger logger = LoggerFactory.getLogger("org.apache.drill.TestReporter");

  static {
    System.setProperty("line.separator", "\n");

    java.lang.Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        Logger socket = LoggerFactory.getLogger("SOCKET");
        if (socket != null) {
          // XXX - Add socket flush here.
        }
      }
    });
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final SystemManager manager = new SystemManager();
  private static final TestLogReporter LOG_OUTCOME = new TestLogReporter();
  private static MemWatcher memWatcher;
  protected static String className;

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(50000);
  @Rule public final TestLogReporter logOutcome = LOG_OUTCOME;
  @Rule public final TestRule REPEAT_RULE = TestTools.getRepeatRule(false);
  @Rule public TestName TEST_NAME = new TestName();

  @ClassRule
  private static final StoreClassNameRule classNameRule = new StoreClassNameRule();

  @BeforeClass
  public static void beforeDrillTestBaseClass() throws Exception {
    memWatcher = new MemWatcher();
  }

  @AfterClass
  public static void afterDrillTestBaseClass() throws InterruptedException{
    logger.info(String.format("Test Class done (%s): %s.", memWatcher.getMemString(true), className));
  }

  @Before
  public void beforeDrillTestBase() throws Exception {
    System.out.printf("Running %s#%s\n", getClass().getName(), TEST_NAME.getMethodName());
  }

  @After
  public void afterDrillTestBase() {
    // Optionally add anything here that needs to be cleared after each test.
  }

  protected static Class<?> getTestClass() {
    return classNameRule.getTestClass();
  }

  /* *** Utility Classes *** */

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

    @Override
    protected void starting(Description description) {
      super.starting(description);
      className = description.getClassName();
      memWatcher = new MemWatcher();
    }

    @Override
    protected void failed(Throwable e, Description description) {
      logger.error(String.format("Test Failed (%s): %s", memWatcher.getMemString(), description.getDisplayName()), e);
    }

    @Override
    public void succeeded(Description description) {
      logger.info(String.format("Test Succeeded (%s): %s", memWatcher.getMemString(), description.getDisplayName()));
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

  /* *** Randomization Utilities *** */

  public static Random random() {
    return RandomizedContext.current().getRandom();
  }

  public static boolean randomBoolean() {
    return random().nextBoolean();
  }

  public static byte randomByte() {
    return (byte) random().nextInt();
  }

  public static short randomShort() {
    return (short) random().nextInt();
  }

  public static int randomInt() {
    return random().nextInt();
  }

  public static float randomFloat() {
    return random().nextFloat();
  }

  public static double randomDouble() {
    return random().nextDouble();
  }

  public static long randomLong() {
    return random().nextLong();
  }

  public static int randomInt(int max) {
    return RandomizedTest.randomInt(max);
  }

  public static int randomIntBetween(int min, int max) {
    return RandomizedTest.randomIntBetween(min, max);
  }

  public static String randomAscii() {
    return RandomizedTest.randomAsciiOfLength(32);
  }

  public static String randomAsciiOfLength(int codeUnits) {
    return RandomizedTest.randomAsciiOfLength(codeUnits);
  }

  public static <T> T randomFrom(List<T> list) {
    return RandomPicks.randomFrom(random(), list);
  }

  public static <T> T randomFrom(T[] array) {
    return RandomPicks.randomFrom(random(), array);
  }

  public static <T> T randomFrom(Collection<T> collection) {
    return RandomPicks.randomFrom(random(), collection);
  }
}
