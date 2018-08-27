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
package org.apache.drill.exec.store.ischema;

import static org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.ischema.InfoSchemaFilter.ExprNode.Type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;

@JsonTypeName("info-schema-filter")
public class InfoSchemaFilter {

  private final ExprNode exprRoot;

  @JsonCreator
  public InfoSchemaFilter(@JsonProperty("exprRoot") ExprNode exprRoot) {
    this.exprRoot = exprRoot;
  }

  @JsonProperty("exprRoot")
  public ExprNode getExprRoot() {
    return exprRoot;
  }

  public static class ExprNode {
    @JsonProperty
    public Type type;

    public ExprNode(Type type) {
      this.type = type;
    }

    public enum Type {
      FUNCTION,
      FIELD,
      CONSTANT
    }
  }

  public static class FunctionExprNode extends ExprNode {
    @JsonProperty
    public String function;

    @JsonProperty
    public List<ExprNode> args;

    @JsonCreator
    public FunctionExprNode(String function, List<ExprNode> args) {
      super(Type.FUNCTION);
      this.function = function;
      this.args = args;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(function);
      builder.append("(");
      builder.append(Joiner.on(",").join(args));
      builder.append(")");
      return builder.toString();
    }
  }

  public static class FieldExprNode extends ExprNode {
    @JsonProperty
    public String field;

    @JsonCreator
    public FieldExprNode(String field) {
      super(Type.FIELD);
      this.field = field;
    }

    @Override
    public String toString() {
      return String.format("Field=%s", field);
    }
  }

  public static class ConstantExprNode extends ExprNode {
    @JsonProperty
    public String value;

    @JsonCreator
    public ConstantExprNode(String value) {
      super(Type.CONSTANT);
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("Literal=%s", value);
    }
  }

  public enum Result {
    TRUE,
    FALSE,
    INCONCLUSIVE;
  }

  /**
   * Evaluate the filter for given <COLUMN NAME, VALUE> pairs.
   * @param recordValues
   * @return
   */
  @JsonIgnore
  public Result evaluate(Map<String, String> recordValues) {
    return evaluateHelper(recordValues, getExprRoot());
  }

  private Result evaluateHelper(Map<String, String> recordValues, ExprNode exprNode) {
    if (exprNode.type == Type.FUNCTION) {
      return evaluateHelperFunction(recordValues, (FunctionExprNode) exprNode);
    }

    throw new UnsupportedOperationException(
        String.format("Unknown expression type '%s' in InfoSchemaFilter", exprNode.type));
  }

  private Result evaluateHelperFunction(Map<String, String> recordValues, FunctionExprNode exprNode) {
    switch(exprNode.function) {
      case "like": {
        FieldExprNode col = (FieldExprNode) exprNode.args.get(0);
        ConstantExprNode pattern = (ConstantExprNode) exprNode.args.get(1);
        ConstantExprNode escape = exprNode.args.size() > 2 ? (ConstantExprNode) exprNode.args.get(2) : null;
        final String fieldValue = recordValues.get(col.field.toString());
        if (fieldValue != null) {
          if (escape == null) {
            return Pattern.matches(sqlToRegexLike(pattern.value).getJavaPatternString(), fieldValue) ?
                Result.TRUE : Result.FALSE;
          } else {
            return Pattern.matches(sqlToRegexLike(pattern.value, escape.value).getJavaPatternString(), fieldValue) ?
                Result.TRUE : Result.FALSE;
          }
        }

        return Result.INCONCLUSIVE;
      }
      case "equal":
      case "not equal":
      case "notequal":
      case "not_equal": {
        FieldExprNode arg0 = (FieldExprNode) exprNode.args.get(0);
        ConstantExprNode arg1 = (ConstantExprNode) exprNode.args.get(1);

        final String value = recordValues.get(arg0.field.toString());
        if (value != null) {
          if (exprNode.function.equals("equal")) {
            return arg1.value.equals(value) ? Result.TRUE : Result.FALSE;
          } else {
            return arg1.value.equals(value) ? Result.FALSE : Result.TRUE;
          }
        }

        return Result.INCONCLUSIVE;
      }

      case "booleanor": {
        // If at least one arg returns TRUE, then the OR function value is TRUE
        // If all args return FALSE, then OR function value is FALSE
        // For all other cases, return INCONCLUSIVE
        Result result = Result.FALSE;
        for(ExprNode arg : exprNode.args) {
          Result exprResult = evaluateHelper(recordValues, arg);
          if (exprResult == Result.TRUE) {
            return Result.TRUE;
          } else if (exprResult == Result.INCONCLUSIVE) {
            result = Result.INCONCLUSIVE;
          }
        }

        return result;
      }

      case "booleanand": {
        // If at least one arg returns FALSE, then the AND function value is FALSE
        // If at least one arg returns INCONCLUSIVE, then the AND function value is INCONCLUSIVE
        // If all args return TRUE, then the AND function value is TRUE
        Result result = Result.TRUE;

        for(ExprNode arg : exprNode.args) {
          Result exprResult = evaluateHelper(recordValues, arg);
          if (exprResult == Result.FALSE) {
            return exprResult;
          }
          if (exprResult == Result.INCONCLUSIVE) {
            result = Result.INCONCLUSIVE;
          }
        }

        return result;
      }

      case "in": {
        FieldExprNode col = (FieldExprNode) exprNode.args.get(0);
        List<ExprNode> args = exprNode.args.subList(1, exprNode.args.size());
        final String fieldValue = recordValues.get(col.field.toString());
        if (fieldValue != null) {
          for(ExprNode arg: args) {
            if (fieldValue.equals(((ConstantExprNode) arg).value)) {
              return Result.TRUE;
            }
          }
          return Result.FALSE;
        }

        return Result.INCONCLUSIVE;
      }
    }

    throw new UnsupportedOperationException(
        String.format("Unknown function '%s' in InfoSchemaFilter", exprNode.function));
  }

  @Override
  public String toString() {
    return exprRoot.toString();
  }
}
