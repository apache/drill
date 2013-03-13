/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.expression.visitors.ExprVisitor;
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
	}
	
	public static class IfCondition{
		public final LogicalExpression condition;
		public final LogicalExpression expression;
		
		public IfCondition(LogicalExpression condition, LogicalExpression expression) {
			//logger.debug("Generating IfCondition {}, {}", condition, expression);
			
			this.condition = condition;
			this.expression = expression;
		}

	}
	
	
	@Override
  public <T> T accept(ExprVisitor<T> visitor) {
    return visitor.visitIfExpression(this);
  }


  public static class Builder{
		List<IfCondition> conditions = new ArrayList<IfCondition>();
		private LogicalExpression elseExpression;
		
		public Builder addCondition(IfCondition condition){
			conditions.add(condition);
            return this;
		}

        public Builder addConditions(Iterable<IfCondition> conditions) {
            for(IfCondition condition : conditions) {
                addCondition(condition);
            }
            return this;
        }
		
		public Builder setElse(LogicalExpression elseExpression) {
			this.elseExpression = elseExpression;
            return this;
		}
		
		public IfExpression build(){
			return new IfExpression(conditions, elseExpression);
		}
		
	}


    @Override
    public DataType getDataType() {
        return DataType.BOOLEAN;
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

    @Override
    public void resolveAndValidate(String expr, ErrorCollector errors) {
    }


    public static Builder newBuilder(){
		return new Builder();
	}


	@Override
	public UnmodifiableIterator<IfCondition> iterator() {
		return conditions.iterator();
	}
	
}
