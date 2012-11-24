package org.apache.drill.common.expression;


public class UnaryFunctions {


  
  
	public static abstract class UnaryLogicalExpressionBase extends LogicalExpressionBase{
		protected final LogicalExpression input1;
		
		protected UnaryLogicalExpressionBase(LogicalExpression input1){
			this.input1 = input1;
		}
		
	  protected void unaryToString(StringBuilder sb, String expr) {
	    sb.append(" ");
	    sb.append(expr);
	    sb.append("( ");
      input1.addToString(sb);
	    sb.append(" ) ");
	  }
		
	}
	
	public static class Not extends UnaryLogicalExpressionBase{

		public Not(LogicalExpression input1) {
			super(input1);
		}

    @Override
    public void addToString(StringBuilder sb) {
      unaryToString(sb, "!");
    }

		
	}
	
	public static class Negative extends UnaryLogicalExpressionBase{

		public Negative(LogicalExpression input1) {
			super(input1);
		}

    @Override
    public void addToString(StringBuilder sb) {
      unaryToString(sb, "-");
    }

    
		@Override
		public DataType getDataType() {
			return input1.getDataType();
		}
	}
}
