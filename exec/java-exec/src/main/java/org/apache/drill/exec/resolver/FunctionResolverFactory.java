package org.apache.drill.exec.resolver;

import org.apache.drill.common.expression.FunctionCall;

public class FunctionResolverFactory {
	
	public static FunctionResolver getResolver(FunctionCall call){
		
		if(call.getDefinition().isOperator()){
			return new OperatorFunctionResolver();
		}
		else {
			return new DefaultFunctionResolver();
		}
		
	}

}
