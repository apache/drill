package org.apache.drill.common.expression;

import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import com.google.common.collect.ImmutableList;

public class ExpressionStringBuilder extends AbstractExprVisitor<Void, StringBuilder, RuntimeException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionStringBuilder.class);

  @Override
  public Void visitFunctionCall(FunctionCall call, StringBuilder sb) throws RuntimeException {
    FunctionDefinition func = call.getDefinition();
    ImmutableList<LogicalExpression> args = call.args;
    if (func.isOperator()) {
      if (args.size() == 1) { // unary
        func.addRegisteredName(sb);
        sb.append("(");
        args.get(0).accept(this, sb);
        sb.append(")");
      } else {
        for (int i = 0; i < args.size(); i++) {
          if (i != 0) {
            sb.append(" ");
            func.addRegisteredName(sb);
          }
          sb.append(" (");
          args.get(i).accept(this, sb);
          sb.append(") ");
        }
      }
    } else { // normal function

      func.addRegisteredName(sb);
      sb.append("(");
      for (int i = 0; i < args.size(); i++) {
        if (i != 0) sb.append(", ");
        args.get(i).accept(this, sb);
      }
      sb.append(") ");
    }
    return null;
  }

  @Override
  public Void visitIfExpression(IfExpression ifExpr, StringBuilder sb) throws RuntimeException {
    ImmutableList<IfCondition> conditions = ifExpr.conditions;
    sb.append(" ( ");
    for(int i =0; i < conditions.size(); i++){
      IfCondition c = conditions.get(i);
      if(i !=0) sb.append(" else ");
      sb.append("if (");
      c.condition.accept(this, sb);
      sb.append(" ) then (");
      c.expression.accept(this, sb);
      sb.append(" ) ");
    }
    sb.append(" end ");
    sb.append(" ) ");
    return null;
  }

  @Override
  public Void visitSchemaPath(SchemaPath path, StringBuilder sb) throws RuntimeException {
    sb.append("'");
    sb.append(path.getPath());
    sb.append("'");
    return null;
  }

  @Override
  public Void visitLongConstant(LongExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append(lExpr.getLong());
    return null;
  }

  @Override
  public Void visitDoubleConstant(DoubleExpression dExpr, StringBuilder sb) throws RuntimeException {
    sb.append(dExpr.getDouble());
    return null;
  }

  @Override
  public Void visitBooleanConstant(BooleanExpression e, StringBuilder sb) throws RuntimeException {
    sb.append(e.getBoolean());
    return null;
  }

  @Override
  public Void visitQuotedStringConstant(QuotedString e, StringBuilder sb) throws RuntimeException {
    sb.append("\"");
    sb.append(e.value);
    sb.append("\"");
    return null;
  }
  
  
}
