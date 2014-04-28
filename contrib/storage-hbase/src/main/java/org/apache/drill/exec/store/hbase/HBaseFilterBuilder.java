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
package org.apache.drill.exec.store.hbase;

import java.util.Arrays;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class HBaseFilterBuilder extends AbstractExprVisitor<HBaseScanSpec, Void, RuntimeException> implements DrillHBaseConstants {

  private static final ImmutableMap<String, String> RELATIONAL_FUNCTIONS_TRANSPOSE_MAP;
  static {
   Builder<String, String> builder = ImmutableMap.builder();
   RELATIONAL_FUNCTIONS_TRANSPOSE_MAP = builder
       .put("equal", "equal")
       .put("not_equal", "not_equal")
       .put("greater_than_or_equal_to", "less_than_or_equal_to")
       .put("greater_than", "less_than")
       .put("less_than_or_equal_to", "greater_than_or_equal_to")
       .put("less_than", "greater_than")
       .build();
  }

  private HBaseScanSpec scanSpec;

  HBaseFilterBuilder(HBaseScanSpec hbaseScanSpec) {
    this.scanSpec = hbaseScanSpec;
  }

  static HBaseScanSpec getHBaseScanSpec(HBaseScanSpec hbaseScanSpec, LogicalExpression e) {
    HBaseFilterBuilder filterBuilder = new HBaseFilterBuilder(hbaseScanSpec);
    return e.accept(filterBuilder, null);
  }

  @Override
  public HBaseScanSpec visitFunctionCall(FunctionCall call, Void filterSet) throws RuntimeException {
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;
    if (args.size() == 2 && RELATIONAL_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)) {
      LogicalExpression nameArg = args.get(0);
      LogicalExpression valueArg = args.get(1);
      if (nameArg instanceof QuotedString) {
        valueArg = nameArg;
        nameArg = args.get(1);
        functionName = RELATIONAL_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
      }

      while (nameArg instanceof CastExpression
          && nameArg.getMajorType().getMinorType() == MinorType.VARCHAR) {
        nameArg = ((CastExpression) nameArg).getInput();
      }

      if (nameArg instanceof FieldReference
          && ((FieldReference) nameArg).getAsUnescapedPath().equals(ROW_KEY)
          && valueArg instanceof QuotedString) {
        return createHBaseScanSpec(functionName , ((QuotedString) valueArg).value.getBytes());
      }
    }
    return null;
  }

  private HBaseScanSpec createHBaseScanSpec(String functionName, byte[] value) {
    byte[] startRow = scanSpec.getStartRow();
    byte[] stopRow = scanSpec.getStopRow();
    Filter filter = null;
    switch (functionName) {
    case "equal":
      startRow = stopRow = value;
      break;
    case "not_equal":
      filter = new RowFilter(CompareOp.NOT_EQUAL, new BinaryComparator(value));
      break;
    case "greater_than_or_equal_to":
      startRow = value;
      filter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(value));
      break;
    case "greater_than":
      startRow = value;
      filter = new RowFilter(CompareOp.GREATER, new BinaryComparator(value));
      break;
    case "less_than_or_equal_to":
      stopRow = Arrays.copyOf(value, value.length+1); // stopRow should be just greater than 'value'
      filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(value));
      break;
    case "less_than":
      stopRow = value;
      filter = new RowFilter(CompareOp.LESS, new BinaryComparator(value));
      break;
    default:
      break;
    }
    if (filter != null || startRow != scanSpec.getStartRow() || stopRow != scanSpec.getStopRow()) {
      return new HBaseScanSpec(scanSpec.getTableName(), startRow, stopRow, filter);
    }
    return null;
  }

  @Override
  public HBaseScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return null;
  }

}
