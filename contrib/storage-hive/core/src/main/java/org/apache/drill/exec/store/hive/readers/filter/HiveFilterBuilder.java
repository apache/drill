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
package org.apache.drill.exec.store.hive.readers.filter;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class HiveFilterBuilder extends AbstractExprVisitor<SearchArgument.Builder, Void, RuntimeException> {

  private static final Logger logger = LoggerFactory.getLogger(HiveFilterBuilder.class);

  private final LogicalExpression le;
  private final SearchArgument.Builder builder;
  private final HashMap<String, SqlTypeName> dataTypeMap;

  private boolean allExpressionsConverted = false;

  HiveFilterBuilder(final LogicalExpression le, final HashMap<String, SqlTypeName> dataTypeMap) {
    this.le = le;
    this.dataTypeMap = dataTypeMap;
    this.builder = SearchArgumentFactory.newBuilder();
  }

  public HiveFilter parseTree() {
    SearchArgument.Builder accept = le.accept(this, null);
    SearchArgument searchArgument = builder.build();
    if (accept != null) {
      searchArgument = accept.build();
    }
    return new HiveFilter(searchArgument);
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public SearchArgument.Builder visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public SearchArgument.Builder visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    if (path instanceof FieldReference) {
      String fieldName = path.getAsNamePart().getName();
      SqlTypeName sqlTypeName = dataTypeMap.get(fieldName);
      switch (sqlTypeName) {
        case BOOLEAN:
          PredicateLeaf.Type valueType = convertLeafType(sqlTypeName);
          builder.startNot().equals(fieldName, valueType, false).end();
          break;
        default:
          // otherwise, we don't know what to do so make it a maybe or do nothing
          builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
      }
    }
    return builder;
  }

  @Override
  public SearchArgument.Builder visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public SearchArgument.Builder visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    String functionName = call.getName();
    List<LogicalExpression> args = call.args();
    if (HiveCompareFunctionsProcessor.isCompareFunction(functionName) || HiveCompareFunctionsProcessor.isIsFunction(functionName) || HiveCompareFunctionsProcessor.isNot(call, functionName)) {
      HiveCompareFunctionsProcessor processor = HiveCompareFunctionsProcessor.createFunctionsProcessorInstance(call);
      if (processor.isSuccess()) {
        return buildSearchArgument(processor);
      }
    } else {
      switch (functionName) {
        case FunctionNames.AND:
          builder.startAnd();
          break;
        case FunctionNames.OR:
          builder.startOr();
          break;
        default:
          logger.warn("Unsupported logical operator:{} for push down", functionName);
          return builder;
      }
      for (LogicalExpression arg : args) {
        arg.accept(this, null);
      }
      builder.end();
    }
    return builder;
  }

  private SearchArgument.Builder buildSearchArgument(final HiveCompareFunctionsProcessor processor) {
    String functionName = processor.getFunctionName();
    SchemaPath field = processor.getPath();
    Object fieldValue = processor.getValue();
    PredicateLeaf.Type valueType = processor.getValueType();
    SqlTypeName sqlTypeName = dataTypeMap.get(field.getAsNamePart().getName());
    if (fieldValue == null) {
      valueType = convertLeafType(sqlTypeName);
    }
    if (valueType == null) {
      return builder;
    }
    switch (functionName) {
      case FunctionNames.EQ:
        builder.startAnd().equals(field.getAsNamePart().getName(), valueType, fieldValue).end();
        break;
      case FunctionNames.NE:
        builder.startNot().equals(field.getAsNamePart().getName(), valueType, fieldValue).end();
        break;
      case FunctionNames.GE:
        builder.startNot().lessThan(field.getAsNamePart().getName(), valueType, fieldValue).end();
        break;
      case FunctionNames.GT:
        builder.startNot().lessThanEquals(field.getAsNamePart().getName(), valueType, fieldValue).end();
        break;
      case FunctionNames.LE:
        builder.startAnd().lessThanEquals(field.getAsNamePart().getName(), valueType, fieldValue).end();
        break;
      case FunctionNames.LT:
        builder.startAnd().lessThan(field.getAsNamePart().getName(), valueType, fieldValue).end();
        break;
      case FunctionNames.IS_NULL:
      case "isNull":
      case "is null":
        builder.startAnd().isNull(field.getAsNamePart().getName(), valueType).end();
        break;
      case FunctionNames.IS_NOT_NULL:
      case "isNotNull":
      case "is not null":
        builder.startNot().isNull(field.getAsNamePart().getName(), valueType).end();
        break;
      case FunctionNames.IS_NOT_TRUE:
      case FunctionNames.IS_FALSE:
      case FunctionNames.NOT:
        builder.startNot().equals(field.getAsNamePart().getName(), valueType, true).end();
        break;
      case FunctionNames.IS_TRUE:
      case FunctionNames.IS_NOT_FALSE:
        builder.startNot().equals(field.getAsNamePart().getName(), valueType, false).end();
        break;
    }
    return builder;
  }

  private PredicateLeaf.Type convertLeafType(SqlTypeName sqlTypeName) {
    PredicateLeaf.Type type = null;
    switch (sqlTypeName) {
      case BOOLEAN:
        type = PredicateLeaf.Type.BOOLEAN;
        break;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        type = PredicateLeaf.Type.LONG;
        break;
      case DOUBLE:
      case FLOAT:
        type = PredicateLeaf.Type.FLOAT;
        break;
      case DECIMAL:
        type = PredicateLeaf.Type.DECIMAL;
        break;
      case DATE:
        type = PredicateLeaf.Type.DATE;
        break;
      case TIMESTAMP:
        type = PredicateLeaf.Type.TIMESTAMP;
        break;
      case CHAR:
      case VARCHAR:
        type = PredicateLeaf.Type.STRING;
        break;
      default:
        logger.warn("Not support push down type:" + sqlTypeName.getName());
    }
    return type;
  }
}
