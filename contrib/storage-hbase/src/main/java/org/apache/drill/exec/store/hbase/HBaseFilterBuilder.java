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

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;

import com.google.common.collect.ImmutableList;

public class HBaseFilterBuilder extends AbstractExprVisitor<HBaseScanSpec, Void, RuntimeException> implements DrillHBaseConstants {

  final private HBaseGroupScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = true;

  HBaseFilterBuilder(HBaseGroupScan groupScan, LogicalExpression le) {
    this.groupScan = groupScan;
    this.le = le;
  }

  public HBaseScanSpec parseTree() {
    HBaseScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getHBaseScanSpec(), parsedSpec);
      /*
       * If RowFilter is THE filter attached to the scan specification,
       * remove it since its effect is also achieved through startRow and stopRow.
       */
      if (parsedSpec.filter instanceof RowFilter) {
        parsedSpec.filter = null;
      }
    }
    return parsedSpec;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public HBaseScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public HBaseScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public HBaseScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    HBaseScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    if (CompareFunctionsProcessor.isCompareFunction(functionName)) {
      /*
       * HBASE-10848: Bug in HBase versions (0.94.[0-18], 0.96.[0-2], 0.98.[0-1])
       * causes a filter with NullComparator to fail. Enable only if specified in
       * the configuration (after ensuring that the HBase cluster has the fix).
       */
      boolean nullComparatorSupported = groupScan.getHBaseConf().getBoolean("drill.hbase.supports.null.comparator", false);

      CompareFunctionsProcessor processor = CompareFunctionsProcessor.process(call, nullComparatorSupported);
      if (processor.isSuccess()) {
        nodeScanSpec = createHBaseScanSpec(processor.getFunctionName(), processor.getPath(), processor.getValue());
      }
    } else {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        HBaseScanSpec leftScanSpec = args.get(0).accept(this, null);
        HBaseScanSpec rightScanSpec = args.get(1).accept(this, null);
        if (leftScanSpec != null && rightScanSpec != null) {
          nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec, rightScanSpec);
        } else {
          allExpressionsConverted = false;
          if ("booleanAnd".equals(functionName)) {
            nodeScanSpec = leftScanSpec == null ? rightScanSpec : leftScanSpec;
          }
        }
        break;
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private HBaseScanSpec mergeScanSpecs(String functionName, HBaseScanSpec leftScanSpec, HBaseScanSpec rightScanSpec) {
    Filter newFilter = null;
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;

    switch (functionName) {
    case "booleanAnd":
      newFilter = HBaseUtils.andFilterAtIndex(leftScanSpec.filter, HBaseUtils.LAST_FILTER, rightScanSpec.filter);
      startRow = HBaseUtils.maxOfStartRows(leftScanSpec.startRow, rightScanSpec.startRow);
      stopRow = HBaseUtils.minOfStopRows(leftScanSpec.stopRow, rightScanSpec.stopRow);
      break;
    case "booleanOr":
      newFilter = HBaseUtils.orFilterAtIndex(leftScanSpec.filter, HBaseUtils.LAST_FILTER, rightScanSpec.filter);
      startRow = HBaseUtils.minOfStartRows(leftScanSpec.startRow, rightScanSpec.startRow);
      stopRow = HBaseUtils.maxOfStopRows(leftScanSpec.stopRow, rightScanSpec.stopRow);
    }
    return new HBaseScanSpec(groupScan.getTableName(), startRow, stopRow, newFilter);
  }

  private HBaseScanSpec createHBaseScanSpec(String functionName, SchemaPath field, byte[] fieldValue) {
    boolean isRowKey = field.getAsUnescapedPath().equals(ROW_KEY);
    if (!(isRowKey
        || (field.getRootSegment().getChild() != null && field.getRootSegment().getChild().isNamed()))) {
      /*
       * if the field in this function is neither the row_key nor a qualified HBase column, return.
       */
      return null;
    }

    CompareOp compareOp = null;
    boolean isNullTest = false;
    WritableByteArrayComparable comparator = new BinaryComparator(fieldValue);
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;
    switch (functionName) {
    case "equal":
      compareOp = CompareOp.EQUAL;
      if (isRowKey) {
        startRow = stopRow = fieldValue;
      }
      break;
    case "not_equal":
      compareOp = CompareOp.NOT_EQUAL;
      break;
    case "greater_than_or_equal_to":
      compareOp = CompareOp.GREATER_OR_EQUAL;
      if (isRowKey) {
        startRow = fieldValue;
      }
      break;
    case "greater_than":
      compareOp = CompareOp.GREATER;
      if (isRowKey) {
        // startRow should be just greater than 'value'
        startRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
      }
      break;
    case "less_than_or_equal_to":
      compareOp = CompareOp.LESS_OR_EQUAL;
      if (isRowKey) {
        // stopRow should be just greater than 'value'
        stopRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
      }
      break;
    case "less_than":
      compareOp = CompareOp.LESS;
      if (isRowKey) {
        stopRow = fieldValue;
      }
      break;
    case "isnull":
    case "isNull":
    case "is null":
      if (isRowKey) {
        return null;
      }
      isNullTest = true;
      compareOp = CompareOp.EQUAL;
      comparator = new NullComparator();
      break;
    case "isnotnull":
    case "isNotNull":
    case "is not null":
      if (isRowKey) {
        return null;
      }
      compareOp = CompareOp.NOT_EQUAL;
      comparator = new NullComparator();
      break;
    }

    if (compareOp != null || startRow != HConstants.EMPTY_START_ROW || stopRow != HConstants.EMPTY_END_ROW) {
      Filter filter = null;
      if (isRowKey) {
        if (compareOp != null) {
          filter = new RowFilter(compareOp, comparator);
        }
      } else {
        byte[] family = HBaseUtils.getBytes(field.getRootSegment().getPath());
        byte[] qualifier = HBaseUtils.getBytes(field.getRootSegment().getChild().getNameSegment().getPath());
        filter = new SingleColumnValueFilter(family, qualifier, compareOp, comparator);
        ((SingleColumnValueFilter)filter).setLatestVersionOnly(true);
        if (!isNullTest) {
          ((SingleColumnValueFilter)filter).setFilterIfMissing(true);
        }
      }
      return new HBaseScanSpec(groupScan.getTableName(), startRow, stopRow, filter);
    }
    // else
    return null;
  }

}
