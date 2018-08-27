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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class RowSetTestUtils {

  private RowSetTestUtils() { }

  public static List<SchemaPath> projectList(String... names) {
    List<SchemaPath> selected = new ArrayList<>();
    for (String name: names) {

      // Parse from string does not handle wildcards.

      if (name.equals(SchemaPath.DYNAMIC_STAR)) {
        selected.add(SchemaPath.STAR_COLUMN);
      } else {
        selected.add(SchemaPath.parseFromString(name));
      }
    }
    return selected;
  }

  public static List<SchemaPath> projectCols(SchemaPath... cols) {
    List<SchemaPath> selected = new ArrayList<>();
    for (SchemaPath col: cols) {
      selected.add(col);
    }
    return selected;
  }

  public static List<SchemaPath> projectAll() {
    return Lists.newArrayList(
        new SchemaPath[] {SchemaPath.getSimplePath(SchemaPath.DYNAMIC_STAR)});
  }

}
