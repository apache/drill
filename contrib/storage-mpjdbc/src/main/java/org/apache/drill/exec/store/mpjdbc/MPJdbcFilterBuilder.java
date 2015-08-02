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

package org.apache.drill.exec.store.mpjdbc;

import java.util.List;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.mpjdbc.MPJdbcScanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class MPJdbcFilterBuilder extends
    AbstractExprVisitor<MPJdbcScanSpec, Void, RuntimeException> {
  static final Logger logger = LoggerFactory
      .getLogger(MPJdbcFilterBuilder.class);
  final MPJdbcGroupScan groupScan;
  final LogicalExpression le;
  private boolean allExpressionsConverted = true;

  public MPJdbcFilterBuilder(MPJdbcGroupScan groupScan,
      LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    this.le = conditionExp;
  }

  public MPJdbcScanSpec parseTree() {
    MPJdbcScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getScanSpec(),
          parsedSpec);
    }
    return parsedSpec;
  }

  private MPJdbcScanSpec mergeScanSpecs(String functionName,
     MPJdbcScanSpec leftScanSpec, MPJdbcScanSpec rightScanSpec) {
    List<String> newFilter;
    switch (functionName) {
    case "booleanAnd":
      if (leftScanSpec.getFilters() != null
          && rightScanSpec.getFilters() != null) {
        /* newFilter = MongoUtils.andFilterAtIndex(leftScanSpec.getFilters(),
            rightScanSpec.getFilters()); */
      } else if (leftScanSpec.getFilters() != null) {
        newFilter = leftScanSpec.getFilters();
      } else {
        newFilter = rightScanSpec.getFilters();
      }
      break;
    case "booleanOr":
     /* newFilter = OdbcUtils.orFilterAtIndex(leftScanSpec.getFilters(),
          rightScanSpec.getFilters()); */
    }
    MPJdbcScanSpec mp =  new MPJdbcScanSpec(groupScan.getScanSpec().getDatabase(), groupScan
        .getScanSpec().getTable(), groupScan.getScanSpec().getColumns());
    return mp;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public MPJdbcScanSpec visitUnknown(LogicalExpression e, Void value)
      throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public MPJdbcScanSpec visitBooleanOperator(BooleanOperator op, Void value) {
    List<LogicalExpression> args = op.args;
    MPJdbcScanSpec nodeScanSpec = null;
    String functionName = op.getName();
    for (int i = 0; i < args.size(); ++i) {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        if (nodeScanSpec == null) {
          nodeScanSpec = args.get(i).accept(this, null);
        } else {
          MPJdbcScanSpec scanSpec = args.get(i).accept(this, null);
          if (scanSpec != null) {
            nodeScanSpec = mergeScanSpecs(functionName, nodeScanSpec, scanSpec);
          } else {
            allExpressionsConverted = false;
          }
        }
        break;
      }
    }
    return nodeScanSpec;
  }

  @Override
  public MPJdbcScanSpec visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {
    MPJdbcScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;
    LogicalExpression nameVal = call.args.get(0);
    LogicalExpression valueVal = null;
    StringBuilder strBuilder = new StringBuilder();
    if(call.args.size() >= 2) {
      valueVal = call.args.get(1);
    }
    logger.info("Name Val:" + nameVal.toString());
    logger.info("Value Val:" + valueVal.toString());

    switch(functionName) {
    case "equal":
      break;
     default:
       break;
    }
    /*
    if (OdbcCompareFunctionProcessor.isCompareFunction(functionName)) {
      OdbcCompareFunctionProcessor processor = OdbcCompareFunctionProcessor
          .process(call);
      if (processor.isSuccess()) {
        try {
          nodeScanSpec = createOdbcScanSpec(processor.getFunctionName(),
              processor.getPath(), processor.getValue());
        } catch (Exception e) {
          logger.error(" Failed to creare Filter ", e);
          // throw new RuntimeException(e.getMessage(), e);
        }
      }
    } else {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        MPJdbcScanSpec leftScanSpec = args.get(0).accept(this, null);
        MPJdbcScanSpec rightScanSpec = args.get(1).accept(this, null);
        if (leftScanSpec != null && rightScanSpec != null) {
          nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec,
              rightScanSpec);
        } else {
          allExpressionsConverted = false;
          if ("booleanAnd".equals(functionName)) {
            nodeScanSpec = leftScanSpec == null ? rightScanSpec : leftScanSpec;
          }
        }
        break;
      }
    }
 */
    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private MPJdbcScanSpec createOdbcScanSpec(String functionName,
      SchemaPath field, Object fieldValue) throws ClassNotFoundException,
      Exception {
    // extract the field name
    String fieldName = field.getAsUnescapedPath();
    /*
    OdbcCompareOp compareOp = null;
    switch (functionName) {
    case "equal":
      compareOp = OdbcCompareOp.EQUAL;
      break;
    case "not_equal":
      compareOp = OdbcCompareOp.NOT_EQUAL;
      break;
    case "greater_than_or_equal_to":
      compareOp = OdbcCompareOp.GREATER_OR_EQUAL;
      break;
    case "greater_than":
      compareOp = OdbcCompareOp.GREATER;
      break;
    case "less_than_or_equal_to":
      compareOp = OdbcCompareOp.LESS_OR_EQUAL;
      break;
    case "less_than":
      compareOp = OdbcCompareOp.LESS;
      break;
    case "isnull":
    case "isNull":
    case "is null":
      compareOp = OdbcCompareOp.IFNULL;
      break;
    case "isnotnull":
    case "isNotNull":
    case "is not null":
      compareOp = OdbcCompareOp.IFNOTNULL;
      break;
    }

    if (compareOp != null) {
      BasicDBObject queryFilter = new BasicDBObject();
      if (compareOp == OdbcCompareOp.IFNULL) {
        queryFilter.put(fieldName,
            new BasicDBObject(OdbcCompareOp.EQUAL.getCompareOp(), null));
      } else if (compareOp == OdbcCompareOp.IFNOTNULL) {
        queryFilter.put(fieldName,
            new BasicDBObject(OdbcCompareOp.NOT_EQUAL.getCompareOp(), null));
      } else {
        queryFilter.put(fieldName, new BasicDBObject(compareOp.getCompareOp(),
            fieldValue));
      }
      return new MPJdbcScanSpec(groupScan.getScanSpec().getDbName(), groupScan
          .getScanSpec().getCollectionName(), queryFilter);
    }
    */
    return null;
  }

}

