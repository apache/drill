package org.apache.drill.common.expression;


public class ExpressionValidationError extends RuntimeException{

	public ExpressionValidationError() {
		super();
	}

	public ExpressionValidationError(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public ExpressionValidationError(String arg0) {
		super(arg0);
	}

	public ExpressionValidationError(Throwable arg0) {
		super(arg0);
	}

}
