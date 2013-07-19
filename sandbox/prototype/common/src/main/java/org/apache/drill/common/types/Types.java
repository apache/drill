package org.apache.drill.common.types;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class Types {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Types.class);
  
  public static final MajorType NULL = required(MinorType.NULL);
  public static final MajorType LATE_BIND_TYPE = optional(MinorType.LATE);
  public static final MajorType REQUIRED_BOOLEAN = required(MinorType.BOOLEAN);
  
  public static enum Comparability{
    UNKNOWN, NONE, EQUAL, ORDERED;
  }
  
  public static boolean isNumericType(MajorType type){
    if(type.getMode() == DataMode.REPEATED) return false;
    
    switch(type.getMinorType()){
    case BIGINT:
    case DECIMAL16:
    case DECIMAL4:
    case DECIMAL8:
    case FLOAT4:
    case FLOAT8:
    case INT:
    case MONEY:
    case SMALLINT:
    case TINYINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
      return true;
      default:
        return false;
    }
  }
  
  public static boolean isFixedWidthType(MajorType type){
    switch(type.getMinorType()){
    case MSGPACK2:
    case MSGPACK4:
    case PROTO2:
    case PROTO4:
    case VARBINARY2:
    case VARBINARY4:
    case VARCHAR2:
    case VARCHAR4:
      return false;
    default:
      return true;
    }
  }
  
  
  public static boolean isStringScalarType(MajorType type){
    if(type.getMode() == DataMode.REPEATED) return false;
    switch(type.getMinorType()){
    case FIXEDCHAR:
    case VARCHAR2:
    case VARCHAR4:
      return true;
    default: 
      return false;
    }
  }
  
  public static boolean isBytesScalarType(MajorType type){
    if(type.getMode() == DataMode.REPEATED) return false;
    switch(type.getMinorType()){
    case FIXEDBINARY:
    case VARBINARY2:
    case VARBINARY4:
      return true;
    default: 
      return false;
    }
  }
  
  public static Comparability getComparability(MajorType type){
    if(type.getMode() == DataMode.REPEATED) return Comparability.NONE;
    if(type.getMinorType() == MinorType.LATE) return Comparability.UNKNOWN;
    
    switch(type.getMinorType()){
    case LATE:
      return Comparability.UNKNOWN;
    case MAP:
    case REPEATMAP:
      return Comparability.NONE;
    case INTERVAL:
    case BOOLEAN:
    case MSGPACK2:
    case MSGPACK4:
    case PROTO2:
    case PROTO4:
      return Comparability.EQUAL;
    default:
      return Comparability.ORDERED;
    }
    
  }
  
  
  public static boolean softEquals(MajorType a, MajorType b, boolean allowNullSwap){
    if(a.getMinorType() != b.getMinorType()) return false;
    if(allowNullSwap){
      switch(a.getMode()){
      case OPTIONAL:
      case REQUIRED:
        switch(b.getMode()){
        case OPTIONAL:
        case REQUIRED:
          return true;
        default:
          return false;
        }
      default:
        return false;
      }
    }else{
      if(a.getMode() != b.getMode()){
        return false;
      }else{
        return true;
      }
    }
  }
  
  public static boolean isLateBind(MajorType type){
    return type.getMinorType() == MinorType.LATE;
  }
  
  public static MajorType required(MinorType type){
    return MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(type).build();
  }
  
  public static MajorType repeated(MinorType type){
    return MajorType.newBuilder().setMode(DataMode.REPEATED).setMinorType(type).build();
  }
  
  public static MajorType optional(MinorType type){
    return MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(type).build();
  }
  
  
}
