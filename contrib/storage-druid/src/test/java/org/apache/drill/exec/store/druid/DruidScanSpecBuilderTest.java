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
package org.apache.drill.exec.store.druid;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.druid.common.DruidConstants;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class DruidScanSpecBuilderTest {

  private static final String SOME_DATASOURCE_NAME = "some datasource";
  private static final long SOME_DATASOURCE_SIZE = 500;
  private static final String SOME_DATASOURCE_MIN_TIME = "some min time";
  private static final String SOME_DATASOURCE_MAX_TIME = "some max time";
  private static final String SOME_FIELD = "some field";
  private static final String SOME_VALUE = "some value";

  private DruidScanSpecBuilder druidScanSpecBuilder;

  @Before
  public void setup() {
    druidScanSpecBuilder = new DruidScanSpecBuilder();
  }

  @Test
  public void buildCalledWithEqualFxShouldBuildSelectorFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder
        .build(
          SOME_DATASOURCE_NAME,
          SOME_DATASOURCE_SIZE,
          SOME_DATASOURCE_MIN_TIME,
          SOME_DATASOURCE_MAX_TIME,
          FunctionNames.EQ,
          schemaPath,
          SOME_VALUE);
    String actual = "{\"type\":\"selector\",\"dimension\":\"some field\",\"value\":\"some value\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithEqualFxIntervalFieldShouldBuildIntervalFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(DruidConstants.INTERVAL_DIMENSION_NAME);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.EQ,
        schemaPath,
        SOME_VALUE);
    String actual = "{\"eventInterval\":\"some value\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithNotEqualFxShouldBuildSelectorFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.NE,
        schemaPath, SOME_VALUE
      );
    String actual = "{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"some field\",\"value\":\"some value\"}}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithGreaterThanOrEqualToFxShouldBuildBoundFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.GE,
        schemaPath,
        SOME_VALUE
      );
    String actual = "{\"type\":\"bound\",\"dimension\":\"some field\",\"lower\":\"some value\",\"ordering\":\"lexicographic\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithGreaterThanFxShouldBuildBoundFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.GT,
        schemaPath,
        SOME_VALUE
      );
    String actual = "{\"type\":\"bound\",\"dimension\":\"some field\",\"lower\":\"some value\",\"lowerStrict\":true,\"ordering\":\"lexicographic\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithGreaterThanFxAndNumericValueShouldBuildBoundFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.GT,
        schemaPath,
        "1"
      );

    String actual = "{\"type\":\"bound\",\"dimension\":\"some field\",\"lower\":\"1\",\"lowerStrict\":true,\"ordering\":\"numeric\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithLessThanOrEqualToFxShouldBuildBoundFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.LE,
        schemaPath,
        SOME_VALUE);

    String actual = "{\"type\":\"bound\",\"dimension\":\"some field\",\"upper\":\"some value\",\"ordering\":\"lexicographic\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithLessThanFxShouldBuildBoundFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.LT,
        schemaPath,
        SOME_VALUE);

    String actual = "{\"type\":\"bound\",\"dimension\":\"some field\",\"upper\":\"some value\",\"upperStrict\":true,\"ordering\":\"lexicographic\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithLessThanFxAndNumericValueShouldBuildBoundFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.LT,
        schemaPath,
        "1");

    String actual = "{\"type\":\"bound\",\"dimension\":\"some field\",\"upper\":\"1\",\"upperStrict\":true,\"ordering\":\"numeric\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithIsNullFxShouldBuildSelectorFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.IS_NULL,
        schemaPath,
        null);
    assertNotNull(druidScanSpec);
    String actual = "{\"type\":\"selector\",\"dimension\":\"some field\",\"value\":null}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithIsNotNullFxShouldBuildSelectorFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder.build(
        SOME_DATASOURCE_NAME,
        SOME_DATASOURCE_SIZE,
        SOME_DATASOURCE_MIN_TIME,
        SOME_DATASOURCE_MAX_TIME,
        FunctionNames.IS_NOT_NULL,
        schemaPath,
        null);
    assertNotNull(druidScanSpec);
    String actual = "{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"some field\",\"value\":null}}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithLikeFxButIfValueIsPrefixedWithRegexKeywordHintShouldBuildRegexFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder
        .build(SOME_DATASOURCE_NAME,
          SOME_DATASOURCE_SIZE,
          SOME_DATASOURCE_MIN_TIME,
          SOME_DATASOURCE_MAX_TIME,
          FunctionNames.LIKE,
          schemaPath,
          "$regex$_some_regular_expression");

    String actual = "{\"type\":\"regex\",\"dimension\":\"some field\",\"pattern\":\"some_regular_expression\"}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }

  @Test
  public void buildCalledWithLikeFxShouldBuildSearchFilter() {
    SchemaPath schemaPath = SchemaPath.getSimplePath(SOME_FIELD);
    DruidScanSpec druidScanSpec =
      druidScanSpecBuilder
        .build(SOME_DATASOURCE_NAME,
          SOME_DATASOURCE_SIZE,
          SOME_DATASOURCE_MIN_TIME,
          SOME_DATASOURCE_MAX_TIME,
          FunctionNames.LIKE,
          schemaPath,
          "some search string");
    String actual = "{\"type\":\"search\",\"dimension\":\"some field\",\"query\":{\"type\":\"contains\",\"value\":\"some search string\",\"caseSensitive\":false}}";
    assertEquals(druidScanSpec.getFilter().toJson(), actual);
  }
}
