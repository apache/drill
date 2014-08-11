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
package org.apache.drill.exec.store.mongo;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.mongo.common.MongoCompareOp;

import com.google.common.collect.ImmutableList;
import com.mongodb.BasicDBObject;

public class MongoFilterBuilder extends
    AbstractExprVisitor<MongoScanSpec, Void, RuntimeException> implements
    DrillMongoConstants {
  final MongoGroupScan groupScan;
  final LogicalExpression le;
  private boolean allExpressionsConverted = true;

  public MongoFilterBuilder(MongoGroupScan groupScan,
      LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    this.le = conditionExp;
  }

  public MongoScanSpec parseTree() {
    MongoScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getScanSpec(),
          parsedSpec);
    }
    return parsedSpec;
  }

  private MongoScanSpec mergeScanSpecs(String functionName, MongoScanSpec leftScanSpec,
      MongoScanSpec rightScanSpec) {
    BasicDBObject newFilter = null;

    switch (functionName) {
    case "booleanAnd":
      newFilter = MongoUtils.andFilterAtIndex(leftScanSpec.getFilters(), rightScanSpec.getFilters());
      break;
    case "booleanOr":
      newFilter = MongoUtils.orFilterAtIndex(leftScanSpec.getFilters(), rightScanSpec.getFilters());
    }
    return new MongoScanSpec(groupScan.getScanSpec().getDbName(), groupScan.getScanSpec().getCollectionName(), newFilter);
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public MongoScanSpec visitUnknown(LogicalExpression e, Void value)
      throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public MongoScanSpec visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {
    MongoScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    if (MongoCompareFunctionProcessor.isCompareFunction(functionName)) {
      MongoCompareFunctionProcessor processor = MongoCompareFunctionProcessor
          .process(call, true);
      if (processor.isSuccess()) {
        nodeScanSpec = createMongoScanSpec(processor.getFunctionName(),
            processor.getPath(), processor.getValue());
      }
    } else {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        MongoScanSpec leftScanSpec = args.get(0).accept(this, null);
        MongoScanSpec rightScanSpec = args.get(1).accept(this, null);
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

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private MongoScanSpec createMongoScanSpec(String functionName,
      SchemaPath field, byte[] fieldValue) {
    // extract the field name
    String fieldName = field.getAsUnescapedPath();
    MongoCompareOp compareOp = null;
    switch (functionName) {
    case "equal":
      compareOp = MongoCompareOp.EQUAL;
      break;
    case "not_equal":
      compareOp = MongoCompareOp.NOT_EQUAL;
      break;
    case "greater_than_or_equal_to":
      compareOp = MongoCompareOp.GREATER_OR_EQUAL;
      break;
    case "greater_than":
      compareOp = MongoCompareOp.GREATER;
      break;
    case "less_than_or_equal_to":
      compareOp = MongoCompareOp.LESS_OR_EQUAL;
      break;
    case "less_than":
      compareOp = MongoCompareOp.LESS;
      break;
    case "isnull":
    case "isNull":
    case "is null":
      compareOp = MongoCompareOp.IFNULL;
      break;
    case "isnotnull":
    case "isNotNull":
    case "is not null":
      compareOp = MongoCompareOp.IFNOTNULL;
      break;
    }

    if(compareOp != null){
      BasicDBObject queryFilter = new BasicDBObject();
      if(compareOp == MongoCompareOp.IFNULL || compareOp == MongoCompareOp.IFNOTNULL){
        // need to verify whether "$eq" or "$ne" needs or not.
        queryFilter.put(fieldName, new BasicDBObject(compareOp.getCompareOp(), fieldValue));
      }else{
        queryFilter.put(fieldName, new BasicDBObject(compareOp.getCompareOp(), fieldValue));
      }
      return new MongoScanSpec(groupScan.getScanSpec().getDbName(), groupScan.getScanSpec().getCollectionName(), queryFilter);
    }
     return null;
  }

}
