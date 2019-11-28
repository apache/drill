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
package org.apache.drill.common.expression;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SchemaPathTest extends BaseTest {

  @Test
  public void testUnIndexedWithOutArray() {
    SchemaPath oneElementSchema = SchemaPath.parseFromString("`a`");
    assertEquals("Schema path should match", oneElementSchema, oneElementSchema.getUnIndexed());

    SchemaPath severalElementsSchema = SchemaPath.parseFromString("`a`.`b`.`c`");
    assertEquals("Schema path should match", severalElementsSchema, severalElementsSchema.getUnIndexed());
  }

  @Test
  public void testUnIndexedEndingWithArray() {
    SchemaPath schemaPath = SchemaPath.parseFromString("`a`.`b`[0]");
    assertEquals("Schema path should match", SchemaPath.parseFromString("`a`.`b`"), schemaPath.getUnIndexed());
  }

  @Test
  public void testUnIndexedArrayInTheMiddle() {
    SchemaPath schemaPath = SchemaPath.parseFromString("`a`.`b`[0].`c`.`d`");
    assertEquals("Schema path should match", SchemaPath.parseFromString("`a`.`b`.`c`.`d`"), schemaPath.getUnIndexed());
  }

  @Test
  public void testUnIndexedMultipleArrays() {
    SchemaPath schemaPath = SchemaPath.parseFromString("`a`.`b`[0][1].`c`.`d`[2][0]");
    assertEquals("Schema path should match", SchemaPath.parseFromString("`a`.`b`.`c`.`d`"), schemaPath.getUnIndexed());
  }

  @Test
  public void testCompoundPathN() {
    SchemaPath schemaPath = SchemaPath.getCompoundPath(3, "a", "b", "c", "d", "e");
    assertEquals("Schema path should match", SchemaPath.getCompoundPath("a", "b", "c"), schemaPath);
  }
}

