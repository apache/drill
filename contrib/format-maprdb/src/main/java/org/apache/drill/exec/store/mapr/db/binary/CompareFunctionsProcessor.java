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
package org.apache.drill.exec.store.mapr.db.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

class CompareFunctionsProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
  private byte[] value;
  private boolean success;
  private boolean isEqualityFn;
  private SchemaPath path;
  private String functionName;
  private boolean sortOrderAscending;

  // Fields for row-key prefix comparison
  // If the query is on row-key prefix, we cannot use a standard template to identify startRow, stopRow and filter
  // Hence, we use these local variables(set depending upon the encoding type in user query)
  private boolean isRowKeyPrefixComparison;
  byte[] rowKeyPrefixStartRow;
  byte[] rowKeyPrefixStopRow;
  Filter rowKeyPrefixFilter;

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  public static CompareFunctionsProcessor process(FunctionCall call, boolean nullComparatorSupported) {
    String functionName = call.getName();
    LogicalExpression nameArg = call.args.get(0);
    LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
    CompareFunctionsProcessor evaluator = new CompareFunctionsProcessor(functionName);

    if (valueArg != null) { // binary function
      if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
        LogicalExpression swapArg = valueArg;
        valueArg = nameArg;
        nameArg = swapArg;
        evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
      }
      evaluator.success = nameArg.accept(evaluator, valueArg);
    } else if (nullComparatorSupported && call.args.get(0) instanceof SchemaPath) {
      evaluator.success = true;
      evaluator.path = (SchemaPath) nameArg;
    }

    return evaluator;
  }

  public CompareFunctionsProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
    this.isEqualityFn = COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)
        && COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName).equals(functionName);
    this.isRowKeyPrefixComparison = false;
    this.sortOrderAscending = true;
  }

  public byte[] getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public String getFunctionName() {
    return functionName;
  }

  public boolean isRowKeyPrefixComparison() {
	return isRowKeyPrefixComparison;
  }

  public byte[] getRowKeyPrefixStartRow() {
    return rowKeyPrefixStartRow;
  }

  public byte[] getRowKeyPrefixStopRow() {
  return rowKeyPrefixStopRow;
  }

  public Filter getRowKeyPrefixFilter() {
  return rowKeyPrefixFilter;
  }

  public boolean isSortOrderAscending() {
    return sortOrderAscending;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
    if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
      return e.getInput().accept(this, valueArg);
    }
    return false;
  }

  @Override
  public Boolean visitConvertExpression(ConvertExpression e, LogicalExpression valueArg) throws RuntimeException {
    if (e.getConvertFunction() == ConvertExpression.CONVERT_FROM) {

      String encodingType = e.getEncodingType();
      int prefixLength    = 0;

      // Handle scan pruning in the following scenario:
      // The row-key is a composite key and the CONVERT_FROM() function has byte_substr() as input function which is
      // querying for the first few bytes of the row-key(start-offset 1)
      // Example WHERE clause:
      // CONVERT_FROM(BYTE_SUBSTR(row_key, 1, 8), 'DATE_EPOCH_BE') < DATE '2015-06-17'
      if (e.getInput() instanceof FunctionCall) {

        // We can prune scan range only for big-endian encoded data
        if (encodingType.endsWith("_BE") == false) {
          return false;
        }

        FunctionCall call = (FunctionCall)e.getInput();
        String functionName = call.getName();
        if (!functionName.equalsIgnoreCase("byte_substr")) {
          return false;
        }

        LogicalExpression nameArg = call.args.get(0);
        LogicalExpression valueArg1 = call.args.size() >= 2 ? call.args.get(1) : null;
        LogicalExpression valueArg2 = call.args.size() >= 3 ? call.args.get(2) : null;

        if (((nameArg instanceof SchemaPath) == false) ||
             (valueArg1 == null) || ((valueArg1 instanceof IntExpression) == false) ||
             (valueArg2 == null) || ((valueArg2 instanceof IntExpression) == false)) {
          return false;
        }

        boolean isRowKey = ((SchemaPath)nameArg).getAsUnescapedPath().equals(DrillHBaseConstants.ROW_KEY);
        int offset = ((IntExpression)valueArg1).getInt();

        if (!isRowKey || (offset != 1)) {
          return false;
        }

        this.path    = (SchemaPath)nameArg;
        prefixLength = ((IntExpression)valueArg2).getInt();
        this.isRowKeyPrefixComparison = true;
        return visitRowKeyPrefixConvertExpression(e, prefixLength, valueArg);
      }

      if (e.getInput() instanceof SchemaPath) {
        ByteBuf bb = null;

        switch (encodingType) {
        case "INT_BE":
        case "INT":
        case "UINT_BE":
        case "UINT":
        case "UINT4_BE":
        case "UINT4":
          if (valueArg instanceof IntExpression
              && (isEqualityFn || encodingType.startsWith("U"))) {
            bb = newByteBuf(4, encodingType.endsWith("_BE"));
            bb.writeInt(((IntExpression)valueArg).getInt());
          }
          break;
        case "BIGINT_BE":
        case "BIGINT":
        case "UINT8_BE":
        case "UINT8":
          if (valueArg instanceof LongExpression
              && (isEqualityFn || encodingType.startsWith("U"))) {
            bb = newByteBuf(8, encodingType.endsWith("_BE"));
            bb.writeLong(((LongExpression)valueArg).getLong());
          }
          break;
        case "FLOAT":
          if (valueArg instanceof FloatExpression && isEqualityFn) {
            bb = newByteBuf(4, true);
            bb.writeFloat(((FloatExpression)valueArg).getFloat());
          }
          break;
        case "DOUBLE":
          if (valueArg instanceof DoubleExpression && isEqualityFn) {
            bb = newByteBuf(8, true);
            bb.writeDouble(((DoubleExpression)valueArg).getDouble());
          }
          break;
        case "TIME_EPOCH":
        case "TIME_EPOCH_BE":
          if (valueArg instanceof TimeExpression) {
            bb = newByteBuf(8, encodingType.endsWith("_BE"));
            bb.writeLong(((TimeExpression)valueArg).getTime());
          }
          break;
        case "DATE_EPOCH":
        case "DATE_EPOCH_BE":
          if (valueArg instanceof DateExpression) {
            bb = newByteBuf(8, encodingType.endsWith("_BE"));
            bb.writeLong(((DateExpression)valueArg).getDate());
          }
          break;
        case "BOOLEAN_BYTE":
          if (valueArg instanceof BooleanExpression) {
            bb = newByteBuf(1, false /* does not matter */);
            bb.writeByte(((BooleanExpression)valueArg).getBoolean() ? 1 : 0);
          }
          break;
        case "DOUBLE_OB":
        case "DOUBLE_OBD":
          if (valueArg instanceof DoubleExpression) {
            bb = newByteBuf(9, true);
            PositionedByteRange br = new SimplePositionedMutableByteRange(bb.array(), 0, 9);
            if (encodingType.endsWith("_OBD")) {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeFloat64(br,
                  ((DoubleExpression)valueArg).getDouble(), Order.DESCENDING);
              this.sortOrderAscending = false;
            } else {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeFloat64(br,
                  ((DoubleExpression)valueArg).getDouble(), Order.ASCENDING);
            }
          }
          break;
        case "FLOAT_OB":
        case "FLOAT_OBD":
          if (valueArg instanceof FloatExpression) {
            bb = newByteBuf(5, true);
            PositionedByteRange br = new SimplePositionedMutableByteRange(bb.array(), 0, 5);
            if (encodingType.endsWith("_OBD")) {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeFloat32(br,
                  ((FloatExpression)valueArg).getFloat(), Order.DESCENDING);
              this.sortOrderAscending = false;
            } else {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeFloat32(br,
                        ((FloatExpression)valueArg).getFloat(), Order.ASCENDING);
            }
          }
          break;
        case "BIGINT_OB":
        case "BIGINT_OBD":
          if (valueArg instanceof LongExpression) {
            bb = newByteBuf(9, true);
            PositionedByteRange br = new SimplePositionedMutableByteRange(bb.array(), 0, 9);
            if (encodingType.endsWith("_OBD")) {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeInt64(br,
                        ((LongExpression)valueArg).getLong(), Order.DESCENDING);
              this.sortOrderAscending = false;
            } else {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeInt64(br,
                  ((LongExpression)valueArg).getLong(), Order.ASCENDING);
            }
          }
          break;
        case "INT_OB":
        case "INT_OBD":
          if (valueArg instanceof IntExpression) {
            bb = newByteBuf(5, true);
            PositionedByteRange br = new SimplePositionedMutableByteRange(bb.array(), 0, 5);
            if (encodingType.endsWith("_OBD")) {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeInt32(br,
                  ((IntExpression)valueArg).getInt(), Order.DESCENDING);
              this.sortOrderAscending = false;
            } else {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeInt32(br,
                        ((IntExpression)valueArg).getInt(), Order.ASCENDING);
            }
          }
          break;
        case "UTF8_OB":
        case "UTF8_OBD":
          if (valueArg instanceof QuotedString) {
            int stringLen = ((QuotedString) valueArg).value.getBytes(Charsets.UTF_8).length;
            bb = newByteBuf(stringLen + 2, true);
            PositionedByteRange br = new SimplePositionedMutableByteRange(bb.array(), 0, stringLen + 2);
            if (encodingType.endsWith("_OBD")) {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeString(br,
                  ((QuotedString)valueArg).value, Order.DESCENDING);
              this.sortOrderAscending = false;
            } else {
              org.apache.hadoop.hbase.util.OrderedBytes.encodeString(br,
                        ((QuotedString)valueArg).value, Order.ASCENDING);
            }
          }
          break;
        case "UTF8":
        // let visitSchemaPath() handle this.
          return e.getInput().accept(this, valueArg);
        }

        if (bb != null) {
          this.value = bb.array();
          this.path = (SchemaPath)e.getInput();
          return true;
        }
      }
    }
    return false;
  }

  private Boolean visitRowKeyPrefixConvertExpression(ConvertExpression e,
    int prefixLength, LogicalExpression valueArg) {
    String encodingType = e.getEncodingType();
    rowKeyPrefixStartRow = HConstants.EMPTY_START_ROW;
    rowKeyPrefixStopRow  = HConstants.EMPTY_START_ROW;
    rowKeyPrefixFilter   = null;

    if ((encodingType.compareTo("UINT4_BE") == 0) ||
        (encodingType.compareTo("UINT_BE") == 0)) {
      if (prefixLength != 4) {
        throw new RuntimeException("Invalid length(" + prefixLength + ") of row-key prefix");
      }

      int val;
      if ((valueArg instanceof IntExpression) == false) {
        return false;
      }

      val = ((IntExpression)valueArg).getInt();

      // For TIME_EPOCH_BE/BIGINT_BE encoding, the operators that we push-down are =, <>, <, <=, >, >=
      switch (functionName) {
      case "equal":
        rowKeyPrefixFilter = new PrefixFilter(ByteBuffer.allocate(4).putInt(val).array());
        rowKeyPrefixStartRow = ByteBuffer.allocate(4).putInt(val).array();
        rowKeyPrefixStopRow = ByteBuffer.allocate(4).putInt(val + 1).array();
        return true;
      case "greater_than_or_equal_to":
        rowKeyPrefixStartRow = ByteBuffer.allocate(4).putInt(val).array();
        return true;
      case "greater_than":
        rowKeyPrefixStartRow = ByteBuffer.allocate(4).putInt(val + 1).array();
        return true;
      case "less_than_or_equal_to":
        rowKeyPrefixStopRow = ByteBuffer.allocate(4).putInt(val + 1).array();
        return true;
      case "less_than":
        rowKeyPrefixStopRow = ByteBuffer.allocate(4).putInt(val).array();
        return true;
      }

      return false;
    }

    if ((encodingType.compareTo("TIMESTAMP_EPOCH_BE") == 0) ||
        (encodingType.compareTo("TIME_EPOCH_BE") == 0) ||
        (encodingType.compareTo("UINT8_BE") == 0)) {

      if (prefixLength != 8) {
        throw new RuntimeException("Invalid length(" + prefixLength + ") of row-key prefix");
      }

      long val;
      if (encodingType.compareTo("TIME_EPOCH_BE") == 0) {
        if ((valueArg instanceof TimeExpression) == false) {
          return false;
        }

        val = ((TimeExpression)valueArg).getTime();
      } else if (encodingType.compareTo("UINT8_BE") == 0){
        if ((valueArg instanceof LongExpression) == false) {
          return false;
        }

        val = ((LongExpression)valueArg).getLong();
      } else if (encodingType.compareTo("TIMESTAMP_EPOCH_BE") == 0) {
        if ((valueArg instanceof TimeStampExpression) == false) {
          return false;
        }

        val = ((TimeStampExpression)valueArg).getTimeStamp();
      } else {
        // Should not reach here.
        return false;
      }

      // For TIME_EPOCH_BE/BIGINT_BE encoding, the operators that we push-down are =, <>, <, <=, >, >=
      switch (functionName) {
      case "equal":
        rowKeyPrefixFilter = new PrefixFilter(ByteBuffer.allocate(8).putLong(val).array());
        rowKeyPrefixStartRow = ByteBuffer.allocate(8).putLong(val).array();
        rowKeyPrefixStopRow = ByteBuffer.allocate(8).putLong(val + 1).array();
        return true;
      case "greater_than_or_equal_to":
        rowKeyPrefixStartRow = ByteBuffer.allocate(8).putLong(val).array();
        return true;
      case "greater_than":
        rowKeyPrefixStartRow = ByteBuffer.allocate(8).putLong(val + 1).array();
        return true;
      case "less_than_or_equal_to":
        rowKeyPrefixStopRow = ByteBuffer.allocate(8).putLong(val + 1).array();
        return true;
      case "less_than":
        rowKeyPrefixStopRow = ByteBuffer.allocate(8).putLong(val).array();
        return true;
      }

      return false;
    }

    if (encodingType.compareTo("DATE_EPOCH_BE") == 0) {
      if ((valueArg instanceof DateExpression) == false) {
        return false;
      }

      if (prefixLength != 8) {
        throw new RuntimeException("Invalid length(" + prefixLength + ") of row-key prefix");
      }

      final long MILLISECONDS_IN_A_DAY  = (long)1000 * 60 * 60 * 24;
      long dateToSet;
      // For DATE encoding, the operators that we push-down are =, <>, <, <=, >, >=
      switch (functionName) {
      case "equal":
        long startDate = ((DateExpression)valueArg).getDate();
        rowKeyPrefixStartRow = ByteBuffer.allocate(8).putLong(startDate).array();
        long stopDate  = ((DateExpression)valueArg).getDate() + MILLISECONDS_IN_A_DAY;
        rowKeyPrefixStopRow = ByteBuffer.allocate(8).putLong(stopDate).array();
        return true;
      case "greater_than_or_equal_to":
        dateToSet = ((DateExpression)valueArg).getDate();
        rowKeyPrefixStartRow = ByteBuffer.allocate(8).putLong(dateToSet).array();
        return true;
      case "greater_than":
        dateToSet = ((DateExpression)valueArg).getDate() + MILLISECONDS_IN_A_DAY;
        rowKeyPrefixStartRow = ByteBuffer.allocate(8).putLong(dateToSet).array();
        return true;
      case "less_than_or_equal_to":
        dateToSet = ((DateExpression)valueArg).getDate() + MILLISECONDS_IN_A_DAY;
        rowKeyPrefixStopRow = ByteBuffer.allocate(8).putLong(dateToSet).array();
        return true;
      case "less_than":
        dateToSet = ((DateExpression)valueArg).getDate();
        rowKeyPrefixStopRow = ByteBuffer.allocate(8).putLong(dateToSet).array();
        return true;
      }

      return false;
    }

    return false;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).value.getBytes(Charsets.UTF_8);
      this.path = path;
      return true;
    }
    return false;
  }

  private static ByteBuf newByteBuf(int size, boolean bigEndian) {
    return Unpooled.wrappedBuffer(new byte[size])
        .order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN)
        .writerIndex(0);
  }

  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
    VALUE_EXPRESSION_CLASSES = builder
        .add(BooleanExpression.class)
        .add(DateExpression.class)
        .add(DoubleExpression.class)
        .add(FloatExpression.class)
        .add(IntExpression.class)
        .add(LongExpression.class)
        .add(QuotedString.class)
        .add(TimeExpression.class)
        .build();
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
        // unary functions
        .put("isnotnull", "isnotnull")
        .put("isNotNull", "isNotNull")
        .put("is not null", "is not null")
        .put("isnull", "isnull")
        .put("isNull", "isNull")
        .put("is null", "is null")
        // binary functions
        .put("like", "like")
        .put("equal", "equal")
        .put("not_equal", "not_equal")
        .put("greater_than_or_equal_to", "less_than_or_equal_to")
        .put("greater_than", "less_than")
        .put("less_than_or_equal_to", "greater_than_or_equal_to")
        .put("less_than", "greater_than")
        .build();
  }

}
