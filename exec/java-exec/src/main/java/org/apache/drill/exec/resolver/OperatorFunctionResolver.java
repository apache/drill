package org.apache.drill.exec.resolver;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

import com.google.common.collect.ImmutableList;

public class OperatorFunctionResolver implements FunctionResolver {

	@Override
	public DrillFuncHolder getBestMatch(List<DrillFuncHolder> methods, FunctionCall call) {
		
		ImmutableList<LogicalExpression> args = call.args;
		
		if(args.size() != 2){
			// TODO: Some Exception
		}	
		
		 
		int bestcost = Integer.MAX_VALUE;
		int currcost = Integer.MAX_VALUE;
		DrillFuncHolder bestmatch = null;
		
		for(DrillFuncHolder h : methods){
			currcost = h.getCost(call);
			
			if(currcost == -1){
				//TODO: check for explicit type cast here
				continue;
			}
			
			if(currcost < bestcost){
				bestcost = currcost;
				bestmatch = h;
			}	      
		}
		
		/** TODO: Convert the function call to new datatypes  **/		
		
		/*if(bestmatch.matches(call)){
	        return bestmatch;
	    }*/
	      
		
		return bestmatch;
	}

}
