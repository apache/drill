package org.apache.drill.exec.resolver;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

public interface FunctionResolver {	

	public DrillFuncHolder getBestMatch(List<DrillFuncHolder> methods, FunctionCall call);

}
