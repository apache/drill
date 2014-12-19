/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.ischema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.ConstantExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.ExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.FieldExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.FunctionExprNode;

import java.util.List;

/**
 * Builds a InfoSchemaFilter out of the Filter condition. Currently we look only for certain conditions. Mainly
 * conditions involving columns "TABLE_NAME", "SCHEMA_NAME" and "TABLE_SCHEMA" and
 * functions EQUAL, NOT EQUAL, LIKE, OR and AND.
 */
public class InfoSchemaFilterBuilder extends AbstractExprVisitor<ExprNode, Void, RuntimeException>
    implements InfoSchemaConstants {
  private final LogicalExpression filter;

  private boolean isAllExpressionsConverted = true;

  public InfoSchemaFilterBuilder(LogicalExpression filter) {
    this.filter = filter;
  }

  public InfoSchemaFilter build() {
    ExprNode exprRoot = filter.accept(this, null);
    if (exprRoot != null) {
      return new InfoSchemaFilter(exprRoot);
    }

    return null;
  }

  public boolean isAllExpressionsConverted() {
    return isAllExpressionsConverted;
  }

  @Override
  public ExprNode visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    final String funcName = call.getName().toLowerCase();
    switch(funcName) {
      case "equal":
      case "not equal":
      case "notequal":
      case "not_equal":
      case "like": {
        ExprNode arg0 = call.args.get(0).accept(this, value);
        ExprNode arg1 = call.args.get(1).accept(this, value);

        if (arg0 != null && arg0 instanceof FieldExprNode && arg1 != null && arg1 instanceof ConstantExprNode) {
          return new FunctionExprNode(funcName, ImmutableList.of(arg0, arg1));
        }
        break;
      }

      case "booleanand": {
        List<ExprNode> args = Lists.newArrayList();
        for(LogicalExpression arg : call.args) {
          ExprNode exprNode = arg.accept(this, value);
          if (exprNode != null && exprNode instanceof FunctionExprNode) {
            args.add(exprNode);
          }
        }
        if (args.size() > 0) {
          return new FunctionExprNode(funcName, args);
        }

        return visitUnknown(call, value);
      }

      case "booleanor": {
        List<ExprNode> args = Lists.newArrayList();
        for(LogicalExpression arg : call.args) {
          ExprNode exprNode = arg.accept(this, value);
          if (exprNode != null && exprNode instanceof FunctionExprNode) {
            args.add(exprNode);
          } else {
            return visitUnknown(call, value);
          }
        }

        if (args.size() > 0) {
          return new FunctionExprNode(funcName, args);
        }

        visitUnknown(call, value);
      }
    }

    return visitUnknown(call, value);
  }

  @Override
  public ExprNode visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public ExprNode visitCastExpression(CastExpression e, Void value) throws RuntimeException {
    if (e.getInput() instanceof FieldReference) {
      FieldReference fieldRef = (FieldReference) e.getInput();
      String field = fieldRef.getAsUnescapedPath().toUpperCase();
      if (field.equals(COL_SCHEMA_NAME) || field.equals(COL_TABLE_NAME) || field.equals(COL_TABLE_SCHEMA)) {
        return new FieldExprNode(field);
      }
    }

    return visitUnknown(e, value);
  }

  @Override
  public ExprNode visitQuotedStringConstant(QuotedString e, Void value) throws RuntimeException {
    return new ConstantExprNode(e.value);
  }

  @Override
  public ExprNode visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    String field = path.getAsUnescapedPath().toUpperCase();
    if (field.equals(COL_SCHEMA_NAME) || field.equals(COL_TABLE_NAME) || field.equals(COL_TABLE_SCHEMA)) {
      return new FieldExprNode(field);
    }

    return visitUnknown(path, value);
  }

  @Override
  public ExprNode visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    isAllExpressionsConverted = false;
    return null;
  }
}
