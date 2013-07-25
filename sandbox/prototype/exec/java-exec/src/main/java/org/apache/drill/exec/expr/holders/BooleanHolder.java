package org.apache.drill.exec.expr.holders;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

public final class BooleanHolder implements ValueHolder{
  public static final MajorType TYPE = Types.required(MinorType.BOOLEAN); 
  public int value;
}