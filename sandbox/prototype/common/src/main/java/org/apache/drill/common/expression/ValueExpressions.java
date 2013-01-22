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

import org.apache.drill.common.expression.visitors.ExprVisitor;





public class ValueExpressions {

  public static LogicalExpression getNumericExpression(String s){
    try{
      long l = Long.parseLong(s);
      return new LongExpression(l);
    }catch(Exception e){
      
    }
    
    try{
      double d = Double.parseDouble(s);
      return new DoubleExpression(d);
    }catch(Exception e){
      
    }
    
    throw new IllegalArgumentException(String.format("Unable to parse string %s as integer or floating point number.", s));
    
  }
  
	protected static abstract class ValueExpression<V> extends
			LogicalExpressionBase {
		public final V value;

		protected ValueExpression(String value) {
			this.value = parseValue(value);
		}

		protected abstract V parseValue(String s);


	}

	public static class BooleanExpression extends ValueExpression<Boolean> {
		public BooleanExpression(String value) {
			super(value);
		}

		@Override
		protected Boolean parseValue(String s) {
			return Boolean.parseBoolean(s);
		}

    @Override
    public void addToString(StringBuilder sb) {
      sb.append(value.toString());
    }

    @Override
    public <T> T accept(ExprVisitor<T> visitor) {
      return visitor.visitBoolean(this);
    }
    
    public boolean getBoolean(){
      return value;
    }
		
	}

	 public static class DoubleExpression extends LogicalExpressionBase  {
	   private double d;
	    public DoubleExpression(double d) {
	      this.d = d;
	    }

	    public double getDouble(){
	      return d;
	    }
	    
	    @Override
	    public void addToString(StringBuilder sb) {
	      sb.append(d);
	    }
	    
	    @Override
	    public <T> T accept(ExprVisitor<T> visitor) {
	      return visitor.visitDoubleExpression(this);
	    }
	  }
	 
	public static class LongExpression extends LogicalExpressionBase {
	  private long l;
		public LongExpression(long l) {
		  this.l = l;
		}
		
		public long getLong(){
		  return l;
		}
		
    @Override
    public void addToString(StringBuilder sb) {
      sb.append(l);
    }
    
    @Override
    public <T> T accept(ExprVisitor<T> visitor) {
      return visitor.visitLongExpression(this);
    }
	}

	public static class QuotedString extends ValueExpression<String> {
		public QuotedString(String value) {
			super(value);
		}

		@Override
		protected String parseValue(String s) {
			return s;
		}
		
    @Override
    public void addToString(StringBuilder sb) {
      sb.append("\"");
      sb.append(value.toString());
      sb.append("\"");
    }
    
    @Override
    public <T> T accept(ExprVisitor<T> visitor) {
      return visitor.visitQuotedString(this);
    }
	}

	
	public static enum CollisionBehavior{
	  SKIP("-"),  // keep the old value.
	  FAIL("!"), // give up on the record
	  REPLACE("+"), // replace the old value with the new value.
	  ARRAYIFY("]"), // replace the current position with an array.  Then place the old and new value in the array. 
	  OBJECTIFY("}"),  // replace the current position with a map.  Give the two values names of 'old' and 'new'. 
	  MERGE_OVERRIDE("%"); // do your best to do a deep merge of the old and new values.
	  
	  private String identifier;
	  
	  private CollisionBehavior(String identifier){
	    this.identifier = identifier;
	  }
	  public static final CollisionBehavior DEFAULT = FAIL;
	  
	  public static final CollisionBehavior find(String c){
	    if(c == null || c.isEmpty()) return DEFAULT;
	    
	    for(CollisionBehavior b : values()){
	      if(b.identifier.equals(c)) return b;
	    }
	    return DEFAULT;
	  }
	}
}
