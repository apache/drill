package org.apache.drill.common.expression;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

public class TypeFunctions {
	public static final Class<?>[] SUB_TYPES = {IsANumber.class};
	

	
	@JsonTypeName("isNumber")
	public static class IsANumber extends FunctionBase{

		public IsANumber(List<LogicalExpression> expressions) {
      super(expressions);
    }

    @Override
		public DataType getDataType() {
			return DataType.BOOLEAN;
		}

    @Override
    public void addToString(StringBuilder sb) {
      this.funcToString(sb, "isNumber");
    }


	}
}
