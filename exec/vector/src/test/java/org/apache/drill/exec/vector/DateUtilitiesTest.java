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
package org.apache.drill.exec.vector;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

public class DateUtilitiesTest {

  /**
   * Test conversion between UTC (what the normal world uses) and Drill's
   * odd "local timestamp."
   * <p>
   * <b>Note:</b> If this fails for you, it may indicate a timezone conversion
   * problem. Don't mark the test as flaky: figure out what's wonky in your
   * timezone. This test was verified in the Pacific time zone.
   */
  @Test
  public void testConversion() {
    // Convert from UTC to a Drill timestamp (in local time)
    Instant nowUtc = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    long drillTs = DateUtilities.utcInstantToDrillTimetamp(nowUtc);

    // Do the conversion with a numeric UTC timestamp
    assertEquals(drillTs, DateUtilities.utcTimestampToDrillTimestamp(nowUtc.toEpochMilli()));

    // Sanity check that we got the right result.
    long offsetSec = DateUtilities.LOCAL_ZONE_ID.getRules().getOffset(Instant.ofEpochMilli(drillTs)).getTotalSeconds();
    long expectedUtc = drillTs - offsetSec * 1000;
    assertEquals(nowUtc.toEpochMilli(), expectedUtc);

    // Now go the other way.
    long utcTs = DateUtilities.drillTimestampToUTCMs(drillTs);
    assertEquals(nowUtc.toEpochMilli(), utcTs);

    Instant recoveredUtcInstant = DateUtilities.drillTimestampToUTCInstant(drillTs);
    assertEquals(nowUtc, recoveredUtcInstant);
  }
}
