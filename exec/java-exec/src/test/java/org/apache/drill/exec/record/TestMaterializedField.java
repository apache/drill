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
package org.apache.drill.exec.record;

import org.apache.drill.categories.VectorTest;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(VectorTest.class)
public class TestMaterializedField {

  private static final String PARENT_NAME = "parent";
  private static final String PARENT_SECOND_NAME = "parent2";
  private static final String CHILD_NAME = "child";
  private static final String CHILD_SECOND_NAME = "child2";
  private static final TypeProtos.MajorType PARENT_TYPE = Types.repeated(TypeProtos.MinorType.MAP);
  private static final TypeProtos.MajorType PARENT_SECOND_TYPE = Types.repeated(TypeProtos.MinorType.LIST);
  private static final TypeProtos.MajorType CHILD_TYPE = Types.repeated(TypeProtos.MinorType.MAP);
  private static final TypeProtos.MajorType CHILD_SECOND_TYPE = Types.repeated(TypeProtos.MinorType.MAP);

  // set of (name, type) tuples representing a test case
  private static final Object[][] matrix = {
      {PARENT_SECOND_NAME, PARENT_TYPE},
      {PARENT_NAME, PARENT_SECOND_TYPE},
      {CHILD_SECOND_NAME, CHILD_TYPE},
      {CHILD_NAME, CHILD_SECOND_TYPE},
  };

  private MaterializedField parent;
  private MaterializedField child;

  @Before
  public void initialize() {
    parent = MaterializedField.create(PARENT_NAME, PARENT_TYPE);
    child = MaterializedField.create(CHILD_NAME, CHILD_TYPE);
    parent.addChild(child);
  }

  @Test
  public void testCopy() {
    final MaterializedField cloneParent = parent.copy();
    assertEquals("Parent copy does not match the original", parent, cloneParent);

    final MaterializedField cloneChild = child.copy();
    assertEquals("Child copy does not match the original", child, cloneChild);

    for (Object[] args : matrix) {
      assertTypeAndPath(parent, (String)args[0], (TypeProtos.MajorType)args[1]);
      assertTypeAndPath(child, (String)args[0], (TypeProtos.MajorType)args[1]);
    }
  }

  private void assertTypeAndPath(MaterializedField field, String path, TypeProtos.MajorType type) {
    final MaterializedField clone = field.copy(path, type);

    assertEquals("field copy path does not match the original", path, clone.getName());
    assertEquals("field copy type does not match the original", type, clone.getType());
    assertEquals("field copy children do not match the original", field.getChildren(), clone.getChildren());
  }
}
