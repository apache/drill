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
package org.apache.drill.exec.planner.sql;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Ticker;
import org.apache.drill.categories.PlannerTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.test.BaseTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(PlannerTest.class)
public class TestPlanCacheConfig extends BaseTest {
  private final ManualTicker ticker = new ManualTicker();
  private PhysicalPlan plan;
  private PhysicalPlanReader reader;
  private PlanCache.ContextSnapshot context;

  @Before
  public void setup() throws Exception {
    plan = mock(PhysicalPlan.class);
    reader = mock(PhysicalPlanReader.class);
    context = mock(PlanCache.ContextSnapshot.class);
    when(reader.writeJson(plan)).thenReturn("{}");
  }

  @Test
  public void testDefaultSettingsKeepHotPlansAndExpireIdlePlans() throws Exception {
    DrillConfig config = DrillConfig.create();
    long maxSizeBytes = config.getLong(ExecConstants.PLAN_CACHE_MAX_SIZE_BYTES);
    Duration write = config.getDuration(ExecConstants.PLAN_CACHE_EXPIRE_AFTER_WRITE);
    Duration access = config.getDuration(ExecConstants.PLAN_CACHE_EXPIRE_AFTER_ACCESS);
    assertEquals(32L * 1024 * 1024, maxSizeBytes);
    assertEquals(Duration.ZERO, write);
    assertEquals(Duration.ofMinutes(10), access);
    try (PlanCache cache = new PlanCache(maxSizeBytes, write, access, ticker)) {
      put(cache, "hot");
      put(cache, "idle");
      for (int i = 0; i < 3; i++) {
        ticker.advance(Duration.ofMinutes(5));
        assertNotNull(cache.get("hot"));
      }
      assertNull(cache.get("idle"));
      ticker.advance(Duration.ofMinutes(10));
      assertNull(cache.get("hot"));
    }
  }

  @Test
  public void testWriteExpirationDoesNotExtendOnAccess() throws Exception {
    try (PlanCache cache = new PlanCache(1024, Duration.ofMinutes(10), Duration.ZERO, ticker)) {
      put(cache, "plan");
      ticker.advance(Duration.ofMinutes(9));
      assertNotNull(cache.get("plan"));
      ticker.advance(Duration.ofMinutes(1));
      assertNull(cache.get("plan"));
    }
  }

  @Test
  public void testReplacementRestartsWriteExpiration() throws Exception {
    try (PlanCache cache = new PlanCache(1024, Duration.ofMinutes(10), Duration.ZERO, ticker)) {
      put(cache, "plan");
      ticker.advance(Duration.ofMinutes(9));
      put(cache, "plan");
      ticker.advance(Duration.ofMinutes(9));
      assertNotNull(cache.get("plan"));
      ticker.advance(Duration.ofMinutes(1));
      assertNull(cache.get("plan"));
    }
  }

  @Test
  public void testCombinedPoliciesExpireAtEitherDeadline() throws Exception {
    try (PlanCache cache = new PlanCache(1024, Duration.ofMinutes(10), Duration.ofMinutes(3), ticker)) {
      put(cache, "idle");
      ticker.advance(Duration.ofMinutes(3));
      assertNull(cache.get("idle"));

      put(cache, "hot");
      for (int i = 0; i < 4; i++) {
        ticker.advance(Duration.ofMinutes(2));
        assertNotNull(cache.get("hot"));
      }
      ticker.advance(Duration.ofMinutes(2));
      assertNull(cache.get("hot"));
    }
  }

  @Test
  public void testBothExpirationPoliciesCanBeDisabled() throws Exception {
    try (PlanCache cache = new PlanCache(1024, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "plan");
      ticker.advance(Duration.ofDays(365));
      assertNotNull(cache.get("plan"));
    }
  }

  @Test
  public void testCapacityCountsUtf8Bytes() throws Exception {
    // Eight JSON bytes plus four key bytes when encoded as UTF-8.
    when(reader.writeJson(plan)).thenReturn("\"ééé\"");
    try (PlanCache cache = new PlanCache(11, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "plan");
      assertNull(cache.get("plan"));
    }
    try (PlanCache cache = new PlanCache(12, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "plan");
      assertNotNull(cache.get("plan"));
    }
  }

  @Test
  public void testCapacityCountsUtf8KeyBytes() throws Exception {
    // Six key bytes plus two JSON bytes.
    try (PlanCache cache = new PlanCache(7, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "ééé");
      assertNull(cache.get("ééé"));
    }
    try (PlanCache cache = new PlanCache(8, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "ééé");
      assertNotNull(cache.get("ééé"));
    }
  }

  @Test
  public void testCapacityCountsUtf8ExplainBytes() throws Exception {
    // Two key bytes, two JSON bytes, and three explain text bytes.
    try (PlanCache cache = new PlanCache(6, Duration.ZERO, Duration.ZERO, ticker)) {
      assertTrue(cache.put("é", reader.writeJson(plan), "计", reader, context));
      assertNull(cache.get("é"));
    }
    try (PlanCache cache = new PlanCache(7, Duration.ZERO, Duration.ZERO, ticker)) {
      assertTrue(cache.put("é", reader.writeJson(plan), "计", reader, context));
      assertNotNull(cache.get("é"));
      assertEquals("计", cache.get("é").getTextPlan());
    }
  }

  @Test
  public void testNullAndEmptyExplainTextHaveZeroWeight() throws Exception {
    try (PlanCache cache = new PlanCache(4, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "é");
      assertNotNull(cache.get("é"));
      assertNull(cache.get("é").getTextPlan());
      assertTrue(cache.put("é", reader.writeJson(plan), "", reader, context));
      assertNotNull(cache.get("é"));
      assertEquals("", cache.get("é").getTextPlan());
    }
  }

  @Test
  public void testCapacityEvictsBeforeIdleExpiration() throws Exception {
    // Small limits use a single Guava segment, making eviction order deterministic.
    try (PlanCache cache = new PlanCache(11, Duration.ZERO, Duration.ofMinutes(10), ticker)) {
      when(reader.writeJson(plan)).thenReturn("\"aa\"");
      put(cache, "a");
      when(reader.writeJson(plan)).thenReturn("\"bbb\"");
      put(cache, "b");
      assertNotNull(cache.get("a"));
      when(reader.writeJson(plan)).thenReturn("\"cc\"");
      put(cache, "c");
      assertNull(cache.get("b"));
      assertNotNull(cache.get("a"));
      assertNotNull(cache.get("c"));
      assertEquals(1L, metric("evictions"));
      assertEquals(2L, metric("entries"));
    }
  }

  @Test
  public void testRegisteredInvalidationAndEntryMetrics() throws Exception {
    try (PlanCache cache = new PlanCache(1024, Duration.ZERO, Duration.ZERO, ticker)) {
      assertEquals(0L, metric("invalidations"));
      assertEquals(0L, metric("entries"));
      put(cache, "key");
      assertEquals(1L, metric("entries"));
      cache.invalidate("key");
      assertEquals(1L, metric("invalidations"));
      assertEquals(0L, metric("entries"));
    }
    assertNull(DrillMetrics.getRegistry().getGauges().get("drill.plan_cache.entries"));
  }

  @Test
  public void testClosingOlderCacheKeepsNewerCacheMetrics() {
    try (PlanCache older = new PlanCache(1024, Duration.ZERO, Duration.ZERO, ticker);
         PlanCache newer = new PlanCache(1024, Duration.ZERO, Duration.ZERO, ticker)) {
      older.close();
      newer.recordBind();
      assertEquals(1L, metric("hits"));
    }
    assertNull(DrillMetrics.getRegistry().getGauges().get("drill.plan_cache.hits"));
  }

  @Test
  public void testZeroCapacityDisablesStorage() throws Exception {
    try (PlanCache cache = new PlanCache(0, Duration.ZERO, Duration.ZERO, ticker)) {
      put(cache, "plan");
      assertNull(cache.get("plan"));
    }
  }

  @Test
  public void testNegativeSettingsAreRejected() {
    IllegalArgumentException size = assertThrows(IllegalArgumentException.class,
        () -> new PlanCache(-1, Duration.ZERO, Duration.ZERO, ticker));
    assertTrue(size.getMessage().contains(ExecConstants.PLAN_CACHE_MAX_SIZE_BYTES));
    IllegalArgumentException write = assertThrows(IllegalArgumentException.class,
        () -> new PlanCache(1024, Duration.ofSeconds(-1), Duration.ZERO, ticker));
    assertTrue(write.getMessage().contains(ExecConstants.PLAN_CACHE_EXPIRE_AFTER_WRITE));
    IllegalArgumentException access = assertThrows(IllegalArgumentException.class,
        () -> new PlanCache(1024, Duration.ZERO, Duration.ofSeconds(-1), ticker));
    assertTrue(access.getMessage().contains(ExecConstants.PLAN_CACHE_EXPIRE_AFTER_ACCESS));
  }

  private void put(PlanCache cache, String key) throws Exception {
    assertTrue(cache.put(key, reader.writeJson(plan), null, reader, context));
  }

  @Test
  public void testClearDiscardsActiveAndQueuedPublications() throws Exception {
    CountDownLatch reading = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    when(reader.readPhysicalPlan("{}")).thenAnswer(invocation -> {
      reading.countDown();
      assertTrue(resume.await(10, TimeUnit.SECONDS));
      return plan;
    });
    try (PlanCache cache = new PlanCache(1024, Duration.ZERO, Duration.ZERO, ticker)) {
      long generation = cache.getGeneration();
      try {
        cache.writeAfterSuccess("active", "{}", null, reader, context, generation);
        assertTrue(reading.await(10, TimeUnit.SECONDS));
        cache.writeAfterSuccess("queued", "{}", null, reader, context, generation);
        cache.clear();
      } finally {
        resume.countDown();
      }
      cache.awaitWrites();
      assertNull(cache.get("active"));
      assertNull(cache.get("queued"));
      assertEquals(0L, metric("entries"));
      assertEquals(1L, metric("invalidations"));
      assertEquals(0L, metric("evictions"));

      // A newly planned query can publish with the new generation.
      cache.writeAfterSuccess("new", "{}", null, reader, context, cache.getGeneration());
      cache.awaitWrites();
      assertNotNull(cache.get("new"));
    }
  }

  private long metric(String name) {
    return (Long) DrillMetrics.getRegistry().getGauges().get("drill.plan_cache." + name).getValue();
  }

  private static final class ManualTicker extends Ticker {
    private long nanos;

    @Override
    public long read() {
      return nanos;
    }

    void advance(Duration duration) {
      nanos += duration.toNanos();
    }
  }
}
