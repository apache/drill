package org.apache.drill.exec.expr.holders;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public class ValueHolderImplmenetations {
  
  private ValueHolderImplmenetations(){}
  
  public final static class BooleanHolder implements ValueHolder{
    public static final MajorType TYPE = Types.required(MinorType.BOOLEAN); 
    public int value;
  }
  
  public final static class NullableBooleanHolder implements ValueHolder {
    public static final MajorType TYPE = Types.optional(MinorType.BOOLEAN);
    public int value;
    public int isSet;
  }
  
  public final static class IntHolder implements ValueHolder{
    public static final MajorType TYPE = Types.required(MinorType.INT); 
    public int value;
  }
  
  public final static class NullableIntHolder implements ValueHolder {
    public static final MajorType TYPE = Types.optional(MinorType.INT);
    public int value;
    public int isSet;
  }

  public final static class LongHolder implements ValueHolder {
    public static final MajorType TYPE = Types.required(MinorType.BIGINT);
    public long value;
  }
  
  public final static class NullableLongHolder implements ValueHolder {
    public static final MajorType TYPE = Types.optional(MinorType.BIGINT);
    public long value;
    public int isSet;
  }
  
}
