package org.apache.drill.exec.expr.holders;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public class Float8Holder implements ValueHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Float8Holder.class);
  
  public static final MajorType TYPE = Types.required(MinorType.FLOAT8);
  public double value;
  
}
