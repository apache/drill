package org.apache.drill.exec.compile;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.CodeGenerator;

import com.sun.codemodel.JType;

public class CodeModelTools {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeModelTools.class);
  
  public static JType getType(MinorType mt, DataMode mode, CodeGenerator g){
    switch (mt) {
    case BOOLEAN:
      return g.getModel().BOOLEAN;
    case INT:
      return g.getModel().INT;
    case BIGINT:
      return g.getModel().LONG;
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  
  public static JType getType(MajorType mt, CodeGenerator g) {
    return getType(mt.getMinorType(), mt.getMode(), g);
  }

}
