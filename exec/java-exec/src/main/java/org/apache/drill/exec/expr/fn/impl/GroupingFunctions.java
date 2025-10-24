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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;

/**
 * Functions for working with GROUPING SETS, ROLLUP, and CUBE.
 *
 * Note: These are internal helper functions. The actual GROUPING() and GROUPING_ID()
 * SQL functions need special query rewriting to work correctly with GROUPING SETS.
 */
public class GroupingFunctions {

  /**
   * GROUPING_ID_INTERNAL - Returns the grouping ID bitmap.
   * This is an internal function that will be called with the $g column value.
   */
  @FunctionTemplate(name = "grouping_id_internal",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GroupingIdInternal implements DrillSimpleFunc {

    @Param IntHolder groupingId;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = groupingId.value;
    }
  }

  /**
   * GROUPING_INTERNAL - Returns 1 if the specified bit in the grouping ID is set, 0 otherwise.
   * This is an internal function that extracts a specific bit from the grouping ID.
   *
   * @param groupingId The grouping ID bitmap ($g column value)
   * @param bitPosition The bit position to check (0-based)
   */
  @FunctionTemplate(name = "grouping_internal",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GroupingInternal implements DrillSimpleFunc {

    @Param IntHolder groupingId;
    @Param IntHolder bitPosition;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      // Extract the bit at bitPosition from groupingId
      // Bit is 1 if column is NOT in the grouping set (i.e., it's a grouping NULL)
      out.value = (groupingId.value >> bitPosition.value) & 1;
    }
  }
}
