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
package org.apache.drill.exec.store.accumulo;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

/**
 * Builds Accumulo scan specifications from Drill filter expressions.
 *
 * <p>This class converts Drill's LogicalExpression filter representation into
 * Accumulo scan parameters (start row, stop row). It focuses on row key
 * predicates since those can be efficiently pushed down to Accumulo's scan range.</p>
 *
 * <p>Supported predicates on row_key:</p>
 * <ul>
 *   <li>row_key = 'value' → exact range</li>
 *   <li>row_key > 'value' → start row (exclusive)</li>
 *   <li>row_key >= 'value' → start row (inclusive)</li>
 *   <li>row_key < 'value' → stop row (exclusive)</li>
 *   <li>row_key <= 'value' → stop row (inclusive)</li>
 *   <li>AND combinations → intersect ranges</li>
 *   <li>OR combinations → union ranges (if contiguous)</li>
 * </ul>
 */
public class AccumuloFilterBuilder
    extends AbstractExprVisitor<AccumuloScanSpec, Void, RuntimeException>
    implements DrillAccumuloConstants {

  private final AccumuloGroupScan groupScan;
  private final LogicalExpression filterExpression;
  private boolean allExpressionsConverted = true;

  public AccumuloFilterBuilder(AccumuloGroupScan groupScan, LogicalExpression filterExpression) {
    this.groupScan = groupScan;
    this.filterExpression = filterExpression;
  }

  /**
   * Parses the filter expression and returns an updated scan specification.
   *
   * @return the scan spec with row key ranges, or null if no filters can be pushed
   */
  public AccumuloScanSpec parseTree() {
    AccumuloScanSpec parsedSpec = filterExpression.accept(this, null);
    if (parsedSpec != null) {
      // Merge with existing scan spec
      parsedSpec = mergeScanSpecs(FunctionNames.AND, groupScan.getScanSpec(), parsedSpec);
    }
    return parsedSpec;
  }

  /**
   * Returns true if all filter expressions were converted to Accumulo scan parameters.
   * If false, the filter operator should remain in the plan for client-side filtering.
   */
  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public AccumuloScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public AccumuloScanSpec visitBooleanOperator(BooleanOperator op, Void value)
      throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public AccumuloScanSpec visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {
    AccumuloScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    List<LogicalExpression> args = call.args();

    if (AccumuloCompareFunctionsProcessor.isCompareFunction(functionName)) {
      AccumuloCompareFunctionsProcessor processor =
          AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);
      if (processor.isSuccess()) {
        nodeScanSpec = createScanSpecFromComparison(processor);
      }
    } else {
      switch (functionName) {
        case FunctionNames.AND:
        case FunctionNames.OR:
          AccumuloScanSpec firstScanSpec = args.get(0).accept(this, null);
          for (int i = 1; i < args.size(); ++i) {
            AccumuloScanSpec nextScanSpec = args.get(i).accept(this, null);
            if (firstScanSpec != null && nextScanSpec != null) {
              nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
            } else {
              allExpressionsConverted = false;
              if (FunctionNames.AND.equals(functionName)) {
                // For AND, keep whichever spec we have
                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
              }
              // For OR, if either is null we can't push down the whole OR
            }
            firstScanSpec = nodeScanSpec;
          }
          break;
        default:
          // Unknown function
          break;
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  /**
   * Creates a scan spec from a comparison processor result.
   */
  private AccumuloScanSpec createScanSpecFromComparison(
      AccumuloCompareFunctionsProcessor processor) {

    String functionName = processor.getFunctionName();
    SchemaPath field = processor.getPath();
    byte[] fieldValue = processor.getValue();

    // Only handle row_key predicates for now
    boolean isRowKey = field.getRootSegmentPath().equalsIgnoreCase(ROW_KEY);
    if (!isRowKey) {
      // Column predicates require iterators - not supported in Option A
      return null;
    }

    byte[] startRow = null;
    byte[] stopRow = null;
    boolean startRowInclusive = true;
    boolean stopRowInclusive = false;

    switch (functionName) {
      case FunctionNames.EQ:
        // row_key = 'value' → scan exactly that row
        startRow = fieldValue;
        // Stop row should be just after the value
        stopRow = Arrays.copyOf(fieldValue, fieldValue.length + 1);
        startRowInclusive = true;
        stopRowInclusive = false;
        break;

      case FunctionNames.NE:
        // row_key != 'value' → can't efficiently push down (would need full scan minus one row)
        return null;

      case FunctionNames.GE:
        // row_key >= 'value' → start at value (inclusive)
        startRow = fieldValue;
        startRowInclusive = true;
        break;

      case FunctionNames.GT:
        // row_key > 'value' → start just after value
        startRow = Arrays.copyOf(fieldValue, fieldValue.length + 1);
        startRowInclusive = true;
        break;

      case FunctionNames.LE:
        // row_key <= 'value' → stop just after value
        stopRow = Arrays.copyOf(fieldValue, fieldValue.length + 1);
        stopRowInclusive = false;
        break;

      case FunctionNames.LT:
        // row_key < 'value' → stop at value (exclusive)
        stopRow = fieldValue;
        stopRowInclusive = false;
        break;

      default:
        return null;
    }

    return new AccumuloScanSpec(
        groupScan.getTableName(),
        startRow,
        stopRow,
        startRowInclusive,
        stopRowInclusive,
        groupScan.getScanSpec().getColumns(),
        null, // No filter expression needed when using row ranges
        groupScan.getScanSpec().getLimit(),
        groupScan.getScanSpec().isUseSortedScanner(),
        groupScan.getScanSpec().isSortDescending());
  }

  /**
   * Merges two scan specs using AND or OR logic.
   */
  private AccumuloScanSpec mergeScanSpecs(
      String functionName,
      AccumuloScanSpec leftSpec,
      AccumuloScanSpec rightSpec) {

    byte[] startRow = null;
    byte[] stopRow = null;
    boolean startRowInclusive = true;
    boolean stopRowInclusive = false;

    switch (functionName) {
      case FunctionNames.AND:
        // AND: Take the intersection (max of starts, min of stops)
        startRow = maxOfStartRows(leftSpec.getStartRow(), rightSpec.getStartRow());
        stopRow = minOfStopRows(leftSpec.getStopRow(), rightSpec.getStopRow());
        break;

      case FunctionNames.OR:
        // OR: Take the union (min of starts, max of stops)
        startRow = minOfStartRows(leftSpec.getStartRow(), rightSpec.getStartRow());
        stopRow = maxOfStopRows(leftSpec.getStopRow(), rightSpec.getStopRow());
        break;

      default:
        return leftSpec;
    }

    return new AccumuloScanSpec(
        leftSpec.getTableName(),
        startRow,
        stopRow,
        startRowInclusive,
        stopRowInclusive,
        leftSpec.getColumns(),
        leftSpec.getFilterExpression(),
        leftSpec.getLimit(),
        leftSpec.isUseSortedScanner(),
        leftSpec.isSortDescending());
  }

  /**
   * Returns the maximum of two start rows (later in sort order).
   */
  private byte[] maxOfStartRows(byte[] left, byte[] right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return compareBytes(left, right) >= 0 ? left : right;
  }

  /**
   * Returns the minimum of two start rows (earlier in sort order).
   */
  private byte[] minOfStartRows(byte[] left, byte[] right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return compareBytes(left, right) <= 0 ? left : right;
  }

  /**
   * Returns the minimum of two stop rows (earlier in sort order).
   */
  private byte[] minOfStopRows(byte[] left, byte[] right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return compareBytes(left, right) <= 0 ? left : right;
  }

  /**
   * Returns the maximum of two stop rows (later in sort order).
   */
  private byte[] maxOfStopRows(byte[] left, byte[] right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return compareBytes(left, right) >= 0 ? left : right;
  }

  /**
   * Compares two byte arrays lexicographically.
   */
  private int compareBytes(byte[] left, byte[] right) {
    int minLen = Math.min(left.length, right.length);
    for (int i = 0; i < minLen; i++) {
      int cmp = (left[i] & 0xFF) - (right[i] & 0xFF);
      if (cmp != 0) {
        return cmp;
      }
    }
    return left.length - right.length;
  }
}
