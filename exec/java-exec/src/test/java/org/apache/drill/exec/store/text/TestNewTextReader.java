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
package org.apache.drill.exec.store.text;

import static org.junit.Assert.assertTrue;

import org.apache.drill.BaseTestQuery;
import org.junit.Assert;
import org.junit.Test;

public class TestNewTextReader extends BaseTestQuery {

  @Test
  public void fieldDelimiterWithinQuotes() throws Exception {
    test("select columns[1] as col1 from cp.`textinput/input1.csv`");
    testBuilder()
        .sqlQuery("select columns[1] as col1 from cp.`textinput/input1.csv`")
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("foo,bar")
        .go();
  }

  @Test
  public void ensureFailureOnNewLineDelimiterWithinQuotes() throws Exception {
    try {
      test("select columns[1] as col1 from cp.`textinput/input2.csv`");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot use newline character within quoted string"));
      return;
    }
    Assert.fail("Expected exception not thrown.");
  }

}
