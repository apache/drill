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
package org.apache.drill.exec.store.mapr.db.json;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.mapr.db.util.FieldPathHelper;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;

import com.google.common.collect.ImmutableList;
import com.mapr.db.MapRDB;

public class JsonConditionBuilder extends AbstractExprVisitor<JsonScanSpec, Void, RuntimeException> implements DrillHBaseConstants {

  final private JsonTableGroupScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = true;

  public JsonConditionBuilder(JsonTableGroupScan groupScan,
      LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    this.le = conditionExp;
  }

  public JsonScanSpec parseTree() {
    JsonScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec.mergeScanSpec("booleanAnd", this.groupScan.getScanSpec());
    }
    return parsedSpec;
  }

  public boolean isAllExpressionsConverted() {
    // TODO Auto-generated method stub
    return allExpressionsConverted;
  }

  @Override
  public JsonScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public JsonScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public JsonScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    JsonScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    if (CompareFunctionsProcessor.isCompareFunction(functionName)) {
      CompareFunctionsProcessor processor = CompareFunctionsProcessor.process(call);
      if (processor.isSuccess()) {
        nodeScanSpec = createJsonScanSpec(call, processor);
      }
    } else {
      switch(functionName) {
      case "booleanAnd":
      case "booleanOr":
        nodeScanSpec = args.get(0).accept(this, null);
        for (int i = 1; i < args.size(); ++i) {
          JsonScanSpec nextScanSpec = args.get(i).accept(this, null);
          if (nodeScanSpec != null && nextScanSpec != null) {
            nodeScanSpec.mergeScanSpec(functionName, nextScanSpec);
        } else {
          allExpressionsConverted = false;
          if ("booleanAnd".equals(functionName)) {
              nodeScanSpec = nodeScanSpec == null ? nextScanSpec : nodeScanSpec;
            }
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

  private void setIsCondition(QueryCondition c,
                              String str,
                              QueryCondition.Op op,
                              Value v) {
    switch (v.getType()) {
    case BOOLEAN:
      c.is(str, op, v.getBoolean());
      break;
    case STRING:
      c.is(str, op, v.getString());
      break;
    case BYTE:
      c.is(str, op, v.getByte());
      break;
    case SHORT:
      c.is(str, op, v.getShort());
      break;
    case INT:
      c.is(str, op, v.getInt());
      break;
    case LONG:
      c.is(str, op, v.getLong());
      break;
    case FLOAT:
      c.is(str, op, v.getFloat());
      break;
    case DOUBLE:
      c.is(str, op, v.getDouble());
      break;
    case DECIMAL:
      c.is(str, op, v.getDecimal());
      break;
    case DATE:
      c.is(str, op, v.getDate());
      break;
    case TIME:
      c.is(str, op, v.getTime());
      break;
    case TIMESTAMP:
      c.is(str, op, v.getTimestamp());
      break;
    case BINARY:
      c.is(str, op, v.getBinary());
      break;
      // XXX/TODO: Map, Array?
    default:
      break;
    }
  }

  private JsonScanSpec createJsonScanSpec(FunctionCall call,
      CompareFunctionsProcessor processor) {
    String functionName = processor.getFunctionName();
    String fieldPath = FieldPathHelper.schemaPathToFieldPath(processor.getPath()).asPathString();
    Value fieldValue = processor.getValue();

    QueryCondition cond = null;
    switch (functionName) {
    case "equal":
      cond = MapRDB.newCondition();
      setIsCondition(cond, fieldPath, Op.EQUAL, fieldValue);
      cond.build();
      break;

    case "not_equal":
      cond = MapRDB.newCondition();
      setIsCondition(cond, fieldPath, Op.NOT_EQUAL, fieldValue);
      cond.build();
      break;

    case "less_than":
      cond = MapRDB.newCondition();
      setIsCondition(cond, fieldPath, Op.LESS, fieldValue);
      cond.build();
      break;

    case "less_than_or_equal_to":
      cond = MapRDB.newCondition();
      setIsCondition(cond, fieldPath, Op.LESS_OR_EQUAL, fieldValue);
      cond.build();
      break;

    case "greater_than":
      cond = MapRDB.newCondition();
      setIsCondition(cond, fieldPath, Op.GREATER, fieldValue);
      cond.build();
      break;

    case "greater_than_or_equal_to":
      cond = MapRDB.newCondition();
      setIsCondition(cond, fieldPath, Op.GREATER_OR_EQUAL, fieldValue);
      cond.build();
      break;

    case "isnull":
      cond = MapRDB.newCondition().notExists(fieldPath).build();
      break;

    case "isnotnull":
      cond = MapRDB.newCondition().exists(fieldPath).build();
      break;

    case "istrue":
      cond = MapRDB.newCondition().is(fieldPath, Op.EQUAL, true).build();
      break;

    case "isnotfalse":
      cond = MapRDB.newCondition().is(fieldPath, Op.NOT_EQUAL, false).build();
      break;

    case "isfalse":
      cond = MapRDB.newCondition().is(fieldPath, Op.EQUAL, false).build();
      break;

    case "isnottrue":
      cond = MapRDB.newCondition().is(fieldPath, Op.NOT_EQUAL, true).build();
      break;

    case "like":
      cond = MapRDB.newCondition().like(fieldPath, fieldValue.getString()).build();
      break;

    default:
    }

    if (cond != null) {
      return new JsonScanSpec(groupScan.getTableName(), cond);
    }

    return null;
  }
}
