package org.apache.drill.common.expression;

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.logical.ValidationError;

public class StringFunctions {

	public static final Class<?>[] SUB_TYPES = {Regex.class, StartsWith.class};
	
	@FunctionName("regex")
	public static class Regex extends FunctionBase{

		
		public Regex(List<LogicalExpression> expressions) {
			super(expressions);

		}

		@Override
		public DataType getDataType() {
			return DataType.NVARCHAR;
		}


    @Override
    public void addToString(StringBuilder sb) {
      this.funcToString(sb, "regex");
    }
    
	}

	@FunctionName("startsWith")
	public static class StartsWith extends FunctionBase{
		
		public StartsWith(List<LogicalExpression> expressions) {
			super(expressions);
		}

		@Override
		public DataType getDataType() {
			return DataType.BOOLEAN;
		}

    @Override
    public void addToString(StringBuilder sb) {
      this.funcToString(sb, "startsWith");
    }

		

	}
	
}
