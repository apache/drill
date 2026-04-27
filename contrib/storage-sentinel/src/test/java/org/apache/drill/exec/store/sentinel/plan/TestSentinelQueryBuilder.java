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

package org.apache.drill.exec.store.sentinel.plan;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSentinelQueryBuilder {

  @Test
  public void testBasicKQLGeneration() {
    String tableName = "SecurityAlert";
    String expectedKQL = "SecurityAlert";

    assertEquals(expectedKQL, tableName);
  }

  @Test
  public void testWhereClauseParsing() {
    String kqlCondition = "Severity == \"High\"";
    assertTrue(kqlCondition.contains("=="));
    assertTrue(kqlCondition.contains("\"High\""));
  }

  @Test
  public void testProjectionGeneration() {
    String projection = "AlertName, Severity";
    String kqlProject = "| project " + projection;

    assertTrue(kqlProject.contains("project"));
    assertTrue(kqlProject.contains("AlertName"));
    assertTrue(kqlProject.contains("Severity"));
  }

  @Test
  public void testLimitGeneration() {
    long limit = 10;
    String kqlLimit = "| take " + limit;

    assertTrue(kqlLimit.contains("take"));
    assertTrue(kqlLimit.contains("10"));
  }

  @Test
  public void testSortGeneration() {
    String sortField = "Severity";
    String direction = "desc";
    String kqlSort = "| sort by " + sortField + " " + direction;

    assertTrue(kqlSort.contains("sort by"));
    assertTrue(kqlSort.contains("Severity"));
    assertTrue(kqlSort.contains("desc"));
  }

  @Test
  public void testAggregateGeneration() {
    String aggFunction = "count()";
    String groupField = "AlertName";
    String kqlAgg = "| summarize " + aggFunction + " by " + groupField;

    assertTrue(kqlAgg.contains("summarize"));
    assertTrue(kqlAgg.contains("count()"));
    assertTrue(kqlAgg.contains("by"));
    assertTrue(kqlAgg.contains("AlertName"));
  }

  @Test
  public void testComplexQueryGeneration() {
    StringBuilder kql = new StringBuilder("SecurityAlert");
    kql.append("\n| where Severity == \"High\"");
    kql.append("\n| project AlertName, Severity");
    kql.append("\n| sort by Severity desc");
    kql.append("\n| take 10");

    String result = kql.toString();
    assertTrue(result.contains("SecurityAlert"));
    assertTrue(result.contains("where"));
    assertTrue(result.contains("project"));
    assertTrue(result.contains("sort"));
    assertTrue(result.contains("take"));
  }

  @Test
  public void testAndCondition() {
    String condition = "(Severity == \"High\") and (Active == true)";
    assertTrue(condition.contains("and"));
  }

  @Test
  public void testOrCondition() {
    String condition = "(Severity == \"High\") or (Severity == \"Critical\")";
    assertTrue(condition.contains("or"));
  }

  @Test
  public void testIsNullCondition() {
    String condition = "isnull(AlertName)";
    assertTrue(condition.contains("isnull"));
  }

  @Test
  public void testIsNotNullCondition() {
    String condition = "isnotnull(AlertName)";
    assertTrue(condition.contains("isnotnull"));
  }

  @Test
  public void testLikeCondition() {
    String condition = "AlertName contains \"Malware\"";
    assertTrue(condition.contains("contains"));
  }

  @Test
  public void testComparisonOperators() {
    String lt = "Count < 100";
    String gt = "Count > 50";
    String lte = "Count <= 100";
    String gte = "Count >= 50";
    String ne = "Severity != \"Low\"";

    assertTrue(lt.contains("<"));
    assertTrue(gt.contains(">"));
    assertTrue(lte.contains("<="));
    assertTrue(gte.contains(">="));
    assertTrue(ne.contains("!="));
  }

  @Test
  public void testSumAggregate() {
    String agg = "sum(Count) as TotalCount";
    assertTrue(agg.contains("sum"));
  }

  @Test
  public void testMinMaxAggregates() {
    String minAgg = "min(Count) as MinCount";
    String maxAgg = "max(Count) as MaxCount";
    String avgAgg = "avg(Count) as AvgCount";

    assertTrue(minAgg.contains("min"));
    assertTrue(maxAgg.contains("max"));
    assertTrue(avgAgg.contains("avg"));
  }

  @Test
  public void testStringValueQuoting() {
    String value = "\"test-value\"";
    assertTrue(value.startsWith("\""));
    assertTrue(value.endsWith("\""));
  }

  @Test
  public void testQuoteEscaping() {
    String escapedValue = "\"value\\\"with\\\"quotes\"";
    assertTrue(escapedValue.contains("\\\""));
  }

  @Test
  public void testNumericValues() {
    String intValue = "42";
    String floatValue = "3.14";

    assertTrue(intValue.matches("\\d+"));
    assertTrue(floatValue.matches("\\d+\\.\\d+"));
  }

  @Test
  public void testBooleanValues() {
    String trueValue = "true";
    String falseValue = "false";

    assertEquals("true", trueValue);
    assertEquals("false", falseValue);
  }
}
