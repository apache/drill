package org.apache.drill.exec.expr.holders;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public class NullableFloat8Holder implements ValueHolder{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableFloat8Holder.class);
  
  public static final MajorType TYPE = Types.optional(MinorType.FLOAT8);
  public double value;
  public int isSet;
  
}
