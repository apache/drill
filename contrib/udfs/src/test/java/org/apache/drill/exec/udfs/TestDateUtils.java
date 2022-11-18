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

package org.apache.drill.exec.udfs;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestDateUtils {

  @Test
  public void testDateFromString() {
    LocalDate testDate = LocalDate.of(2022, 3,14);
    assertEquals(testDate, DateUtilFunctions.getDateFromString("2022-03-14"));
    assertEquals(testDate, DateUtilFunctions.getDateFromString("3/14/2022"));
    assertEquals(testDate, DateUtilFunctions.getDateFromString("14/03/2022", true));
    assertEquals(testDate, DateUtilFunctions.getDateFromString("2022/3/14"));

    // Test bad dates
    assertNull(DateUtilFunctions.getDateFromString(null));
    assertNull(DateUtilFunctions.getDateFromString("1975-13-56"));
    assertNull(DateUtilFunctions.getDateFromString("1975-1s"));
  }

  @Test
  public void testTimestampFromString() {
    LocalDateTime testNoSecondFragments = LocalDateTime.of(2022,4,19,17,3,46);
    LocalDateTime test1Digit = LocalDateTime.of(2022,4,19,17,3,46, 1000000);
    LocalDateTime test2Digit = LocalDateTime.of(2022,4,19,17,3,46, 13000000);
    LocalDateTime test3Digit = LocalDateTime.of(2022,4,19,17,3,46, 342000000);

    assertEquals(testNoSecondFragments,
      DateUtilFunctions.getTimestampFromString("2022-04-19 17:03:46"));
    assertEquals(testNoSecondFragments,
      DateUtilFunctions.getTimestampFromString("2022-04-19T17:03:46"));
    assertEquals(testNoSecondFragments,
      DateUtilFunctions.getTimestampFromString("2022-04-19T17:03:46.000Z"));
    assertEquals(test1Digit,
      DateUtilFunctions.getTimestampFromString("2022-04-19T17:03:46.1Z"));
    assertEquals(test1Digit,
      DateUtilFunctions.getTimestampFromString("2022-04-19T17:03:46.001Z"));
    assertEquals(test2Digit,
      DateUtilFunctions.getTimestampFromString("2022-04-19T17:03:46.13Z"));
    assertEquals(test2Digit,
      DateUtilFunctions.getTimestampFromString("2022-04-19T17:03:46.013Z"));
    assertEquals(test3Digit,
      DateUtilFunctions.getTimestampFromString("2022-04-19 17:03:46.342Z"));

    // Test bad dates
    assertNull(DateUtilFunctions.getTimestampFromString(null));
    assertNull(DateUtilFunctions.getTimestampFromString(""));
  }
}
