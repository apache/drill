package org.apache.drill.common.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;

public class IfExpression extends LogicalExpressionBase implements Iterable<IfCondition>{
	static final Logger logger = LoggerFactory.getLogger(IfExpression.class);
	
	public final ImmutableList<IfCondition> conditions;
	public final LogicalExpression elseExpression;
	
	private IfExpression(List<IfCondition> conditions, LogicalExpression elseExpression){
		this.conditions = ImmutableList.copyOf(conditions);
		this.elseExpression = elseExpression;
	};
	
	
	public static class IfCondition{
		public final LogicalExpression condition;
		public final LogicalExpression expression;
		
		public IfCondition(LogicalExpression condition,
				LogicalExpression expression) {
			logger.debug("Generating IfCondition {}, {}", condition, expression);
			
			this.condition = condition;
			this.expression = expression;
		}

	}
	
	public static class Builder{
		List<IfCondition> conditions = new ArrayList<IfCondition>();
		private LogicalExpression elseExpression;
		
		public void addCondition(IfCondition condition){
			conditions.add(condition);
		}
		
		public void setElse(LogicalExpression elseExpression) {
			this.elseExpression = elseExpression;
		}
		
		public IfExpression build(){
			return new IfExpression(conditions, elseExpression);
		}
		
	}
	
	
	
	
	@Override
  public void addToString(StringBuilder sb) {
	  sb.append(" ( ");
	  for(int i =0; i < conditions.size(); i++){
	    IfCondition c = conditions.get(i);
	    if(i !=0) sb.append(" else ");
	    sb.append("if (");
	    c.condition.addToString(sb);
	    sb.append(" ) then (");
	    c.expression.addToString(sb);
	    sb.append(" ) ");
	  }
	  sb.append(" end ");
	  sb.append(" ) ");
  }


  public static Builder newBuilder(){
		return new Builder();
	}


	@Override
	public UnmodifiableIterator<IfCondition> iterator() {
		return conditions.iterator();
	}
	
}
