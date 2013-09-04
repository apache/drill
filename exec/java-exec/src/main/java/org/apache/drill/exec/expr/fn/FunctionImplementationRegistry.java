package org.apache.drill.exec.expr.fn;

import java.util.List;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ArrayListMultimap;

public class FunctionImplementationRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);
  
  private ArrayListMultimap<String, DrillFuncHolder> methods = ArrayListMultimap.create();
  
  public FunctionImplementationRegistry(DrillConfig config){
    FunctionConverter converter = new FunctionConverter();
    Set<Class<? extends DrillFunc>> providerClasses = PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));
    for (Class<? extends DrillFunc> clazz : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(clazz);
      if(holder != null){
        methods.put(holder.getFunctionName(), holder);
//        logger.debug("Registering function {}", holder);
      }else{
        logger.warn("Unable to initialize function for class {}", clazz.getName());
      }
    }
  }
  
  public DrillFuncHolder getFunction(FunctionCall call){
    for(DrillFuncHolder h : methods.get(call.getDefinition().getName())){
      if(h.matches(call)){
        return h;
      }
    }
    List<MajorType> types = Lists.newArrayList();
    for(LogicalExpression e : call.args){
      types.add(e.getMajorType());
    }
    StringBuilder sb = new StringBuilder();
    sb.append("Missing function implementation: ");
    sb.append("[");
    sb.append(call.getDefinition().getName());
    sb.append("(");
    boolean first = true;
    for(MajorType mt : types){
      if(first){
        first = false;
      }else{
        sb.append(", ");
      }
      sb.append(mt.getMinorType().name());
      sb.append("-");
      sb.append(mt.getMode().name());
    }
    sb.append(")");
    sb.append("]");
    throw new UnsupportedOperationException(sb.toString());
  }

  

}
