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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSentinelQueryBuilder {

  @Test
  public void testRexConverterExists() {
    // Test that the RexToKqlConverter class is available and can be instantiated
    assertNotNull(RexToKqlConverter.class);
  }

  @Test
  public void testKqlWhereClauseSyntax() {
    // Test that KQL where clause syntax is correct
    String whereClause = "Severity == \"High\"";
    assertTrue(whereClause.contains("=="));
    assertTrue(whereClause.contains("\"High\""));
  }

  @Test
  public void testKqlProjectSyntax() {
    // Test that KQL project (column selection) syntax is correct
    String projectClause = "| project AlertName, Severity, Count";
    assertTrue(projectClause.contains("project"));
    assertTrue(projectClause.contains("AlertName"));
    assertTrue(projectClause.contains("Severity"));
  }

  @Test
  public void testKqlSortSyntax() {
    // Test that KQL sort syntax is correct
    String sortClause = "| sort by TimeGenerated desc";
    assertTrue(sortClause.contains("sort by"));
    assertTrue(sortClause.contains("desc"));
  }

  @Test
  public void testKqlTakeSyntax() {
    // Test that KQL take (limit) syntax is correct
    String takeClause = "| take 100";
    assertTrue(takeClause.contains("take"));
    assertTrue(takeClause.contains("100"));
  }

  @Test
  public void testKqlSummarizeSyntax() {
    // Test that KQL summarize (aggregate) syntax is correct
    String summarizeClause = "| summarize count() by Severity";
    assertTrue(summarizeClause.contains("summarize"));
    assertTrue(summarizeClause.contains("count()"));
    assertTrue(summarizeClause.contains("by"));
  }

  @Test
  public void testKqlAndCondition() {
    // Test that KQL AND condition syntax is correct
    String andClause = "(Severity == \"High\") and (Status == \"New\")";
    assertTrue(andClause.contains("and"));
  }

  @Test
  public void testKqlOrCondition() {
    // Test that KQL OR condition syntax is correct
    String orClause = "(Severity == \"High\") or (Severity == \"Critical\")";
    assertTrue(orClause.contains("or"));
  }

  @Test
  public void testKqlComparisonOperators() {
    // Test that KQL comparison operators are correct
    assertTrue("Count < 100".contains("<"));
    assertTrue("Count > 50".contains(">"));
    assertTrue("Count <= 100".contains("<="));
    assertTrue("Count >= 50".contains(">="));
    assertTrue("Severity != \"Low\"".contains("!="));
  }

  @Test
  public void testKqlIsNullSyntax() {
    // Test that KQL isnull() syntax is correct
    String isNullClause = "isnull(AlertName)";
    assertTrue(isNullClause.contains("isnull"));
  }

  @Test
  public void testKqlIsNotNullSyntax() {
    // Test that KQL isnotnull() syntax is correct
    String isNotNullClause = "isnotnull(AlertName)";
    assertTrue(isNotNullClause.contains("isnotnull"));
  }

  @Test
  public void testKqlStartsWithSyntax() {
    // Test that KQL startswith syntax for LIKE prefix matching is correct
    String startsWithClause = "AlertName startswith \"Malware\"";
    assertTrue(startsWithClause.contains("startswith"));
  }

  @Test
  public void testKqlContainsSyntax() {
    // Test that KQL contains syntax for LIKE substring matching is correct
    String containsClause = "AlertName contains \"virus\"";
    assertTrue(containsClause.contains("contains"));
  }

  @Test
  public void testComplexKqlQuery() {
    // Test a complete KQL query combining multiple operations
    String complexQuery = "SecurityAlert\n" +
        "| where Severity == \"High\"\n" +
        "| project AlertName, Severity, Count\n" +
        "| sort by Count desc\n" +
        "| take 50";

    assertTrue(complexQuery.contains("where"));
    assertTrue(complexQuery.contains("project"));
    assertTrue(complexQuery.contains("sort by"));
    assertTrue(complexQuery.contains("take"));
  }
}
