package org.apache.drill.common.expression;

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.logical.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;



public class MathFunction extends LogicalExpressionBase{
	
	static final Logger logger = LoggerFactory.getLogger(MathFunction.class);
	
	public final Method method;
	public final LogicalExpression left;
	public final LogicalExpression right;

	public MathFunction(Method method, LogicalExpression left, LogicalExpression right) throws ExpressionValidationError {
		this.method = method;
		this.left = left;
		this.right = right;
	}
	
	public MathFunction(String methodString, LogicalExpression left, LogicalExpression right) throws ExpressionValidationError {
		logger.debug("{}|{} Generating new Math expression of type " + methodString, left, right);
		Method temp = null;
		for(Method m : Method.values()){
			if(m.expr.equals(methodString)){
				temp = m;
				break;
			}
		}		
		if(temp == null) throw new IllegalArgumentException("Unknown match operator: " + methodString);
		this.method = temp;
		this.left = left;
		this.right = right;
	}

  @Override
  public void addToString(StringBuilder sb) {
    sb.append(" ( ");
    left.addToString(sb);
    sb.append(" ");
    sb.append(method.expr);
    sb.append(" ");
    right.addToString(sb);
    sb.append(" ) ");
  }

  
	public static enum Method{
		ADD("+"), DIVIDE("/"), MULTIPLY("*"), SUBSTRACT("-"), POWER("^"), MOD("%");
		public final String expr;
		
		Method(String expr){
			this.expr = expr;
		}
	}

	
	public static LogicalExpression create(List<LogicalExpression> expressions, List<String> operators){
		
		if(expressions.size() == 1){
			return expressions.get(0);
		}
		
		if(expressions.size()-1 != operators.size()) throw new IllegalArgumentException("Must receive one more expression then the provided number of operators.");
		
		LogicalExpression first = expressions.get(0);
		LogicalExpression second;
		for(int i=0; i < operators.size(); i ++){
			second = expressions.get(i+1);
			first = new MathFunction(operators.get(i), first, second );
		}
		return first;
	}


	
}
