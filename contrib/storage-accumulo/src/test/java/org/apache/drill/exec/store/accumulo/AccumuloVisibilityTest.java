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
package org.apache.drill.exec.store.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Verifies that scans honour Accumulo's column visibility labels.
 *
 * <p>Scanners are created with the connecting user's authorizations, so a query
 * returns the entries that user is cleared to see: unlabeled entries plus those
 * whose label the user has been granted. Entries carrying a label the user does
 * not hold stay hidden. Creating the scanner with {@code Authorizations.EMPTY}
 * instead would drop every labeled entry, including ones the user is entitled
 * to read.</p>
 */
public class AccumuloVisibilityTest extends BaseAccumuloTest {

  @Test
  public void testScanReturnsEntriesTheUserIsAuthorizedFor() throws Exception {
    String sql = "SELECT " + utf8("row_key", "key") + ", " + utf8("t.cf.name", "name")
        + fromTable(AccumuloTestUtils.TEST_TABLE_VISIBILITY) + " ORDER BY row_key";

    List<List<String>> rows = runAndReadStrings(sql);

    assertEquals(Arrays.asList(
        Arrays.asList("vis_001", "unlabeled"),
        Arrays.asList("vis_002", "readable")), rows);
  }

  @Test
  public void testScanHidesEntriesTheUserIsNotAuthorizedFor() throws Exception {
    String sql = "SELECT " + utf8("row_key", "key")
        + fromTable(AccumuloTestUtils.TEST_TABLE_VISIBILITY)
        + " WHERE row_key = 'vis_003'";

    assertEquals(0, runAndReadStrings(sql).size());
  }
}
