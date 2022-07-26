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

package org.apache.drill.exec.store.googlesheets;

import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsRangeBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRangeBuilder {

  @Test
  public void testBasicRange() {
    GoogleSheetsRangeBuilder rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 10_000);
    assertEquals("'Sheet1'!1:10000", rangeBuilder.next());
    assertEquals("'Sheet1'!10001:20000", rangeBuilder.next());
    assertEquals("'Sheet1'!20001:30000", rangeBuilder.next());
    rangeBuilder.lastBatch();
    assertNull(rangeBuilder.next());
  }

  @Test
  public void testRangeWithLimit() {
    GoogleSheetsRangeBuilder rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 100)
      .addLimit(204);

    assertEquals("'Sheet1'!1:100", rangeBuilder.next());
    assertEquals("'Sheet1'!101:200", rangeBuilder.next());
    assertEquals("'Sheet1'!201:204", rangeBuilder.next());
    // Limit reached... no more results.
    assertNull(rangeBuilder.next());
  }

  @Test
  public void testRangeWithColumnsAndRowCount() {
    GoogleSheetsRangeBuilder rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 10);
    rangeBuilder.addFirstColumn("A")
      .addLastColumn("F")
      .isStarQuery(true)
      .addRowCount(25);
    assertEquals("'Sheet1'!A1:F10", rangeBuilder.next());
    assertEquals("'Sheet1'!A11:F20", rangeBuilder.next());
    assertEquals("'Sheet1'!A21:F25", rangeBuilder.next());
    assertNull(rangeBuilder.next());



  }

  @Test
  public void testRangeWithColumnsAndLimitAndRowCount() {
    GoogleSheetsRangeBuilder rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 100);
    rangeBuilder.addFirstColumn("A")
      .addLastColumn("F")
      .isStarQuery(true)
      .addRowCount(25);
    assertEquals("'Sheet1'!A1:F25", rangeBuilder.next());
    // Row count reached... no more records
    assertNull(rangeBuilder.next());

    rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 100);
    rangeBuilder.addFirstColumn("A")
      .addLastColumn("F")
      .isStarQuery(true)
      .addRowCount(25)
      .addLimit(17);
    assertEquals("'Sheet1'!A1:F17", rangeBuilder.next());
    assertNull(rangeBuilder.next());

    rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 100);
    rangeBuilder.addFirstColumn("A")
      .addLastColumn("F")
      .isStarQuery(true)
      .addRowCount(150)
      .addLimit(125);
    assertEquals("'Sheet1'!A1:F100", rangeBuilder.next());
    assertEquals("'Sheet1'!A101:F125", rangeBuilder.next());
    assertNull(rangeBuilder.next());

    rangeBuilder = new GoogleSheetsRangeBuilder("Sheet1", 100);
    rangeBuilder.addFirstColumn("A")
      .addLastColumn("F")
      .isStarQuery(true)
      .addRowCount(125)
      .addLimit(150);
    assertEquals("'Sheet1'!A1:F100", rangeBuilder.next());
    assertEquals("'Sheet1'!A101:F125", rangeBuilder.next());
    assertNull(rangeBuilder.next());
  }
}
