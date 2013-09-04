package org.apache.drill.common.types;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

import static org.apache.drill.common.types.TypeProtos.DataMode.REPEATED;

public class Types {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Types.class);
  
  public static final MajorType NULL = required(MinorType.NULL);
  public static final MajorType LATE_BIND_TYPE = optional(MinorType.LATE);
  public static final MajorType REQUIRED_BIT = required(MinorType.BIT);
  
  public static enum Comparability{
    UNKNOWN, NONE, EQUAL, ORDERED;
  }
  
  public static boolean isNumericType(MajorType type){
    if(type.getMode() == REPEATED) return false;
    
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
  
  public static boolean usesHolderForGet(MajorType type){
    if(type.getMode() == REPEATED) return true;
    switch(type.getMinorType()){
    case BIGINT:
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
      return false;
    
    default: 
      return true;
    }
    
  }
  
  public static boolean isFixedWidthType(MajorType type){
    switch(type.getMinorType()){
    case VARBINARY:
    case VAR16CHAR:
    case VARCHAR:
      return false;
    default:
      return true;
    }
  }
  
  
  public static boolean isStringScalarType(MajorType type){
    if(type.getMode() == REPEATED) return false;
    switch(type.getMinorType()){
    case FIXEDCHAR:
    case FIXED16CHAR:
    case VARCHAR:
    case VAR16CHAR:
      return true;
    default: 
      return false;
    }
  }
  
  public static boolean isBytesScalarType(MajorType type){
    if(type.getMode() == REPEATED) return false;
    switch(type.getMinorType()){
    case FIXEDBINARY:
    case VARBINARY:
      return true;
    default: 
      return false;
    }
  }
  
  public static Comparability getComparability(MajorType type){
    if(type.getMode() == REPEATED) return Comparability.NONE;
    if(type.getMinorType() == MinorType.LATE) return Comparability.UNKNOWN;
    
    switch(type.getMinorType()){
    case LATE:
      return Comparability.UNKNOWN;
    case MAP:
    case REPEATMAP:
      return Comparability.NONE;
    case INTERVAL:
    case BIT:
      return Comparability.EQUAL;
    default:
      return Comparability.ORDERED;
    }
    
  }
  
  
  public static boolean softEquals(MajorType a, MajorType b, boolean allowNullSwap){
    if(a.getMinorType() != b.getMinorType()){
      if(
          (a.getMinorType() == MinorType.VARBINARY && b.getMinorType() == MinorType.VARCHAR) ||
          (b.getMinorType() == MinorType.VARBINARY && a.getMinorType() == MinorType.VARCHAR) 
          ){
        // fall through;
      }else{
        return false;  
      }
      
    }
    if(allowNullSwap){
      switch(a.getMode()){
      case OPTIONAL:
      case REQUIRED:
        switch(b.getMode()){
        case OPTIONAL:
        case REQUIRED:
          return true;
        }
      }
    }
    return a.getMode() == b.getMode();
  }
  
  public static boolean isLateBind(MajorType type){
    return type.getMinorType() == MinorType.LATE;
  }
  
  public static MajorType required(MinorType type){
    return MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(type).build();
  }
  
  public static MajorType repeated(MinorType type){
    return MajorType.newBuilder().setMode(REPEATED).setMinorType(type).build();
  }
  
  public static MajorType optional(MinorType type){
    return MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(type).build();
  }

  public static MajorType overrideMinorType(MajorType originalMajorType, MinorType overrideMinorType) {
    switch(originalMajorType.getMode()) {
      case REPEATED:
        return repeated(overrideMinorType);
      case OPTIONAL:
        return optional(overrideMinorType);
      case REQUIRED:
        return required(overrideMinorType);
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  
}
