package org.apache.drill.exec.record;

import org.apache.drill.common.types.TypeProtos.MajorType;

public class TypedFieldId {
  final MajorType type;
  final int fieldId;
  final boolean isHyperReader;

  public TypedFieldId(MajorType type, int fieldId){
    this(type, fieldId, false);
  }
  
  public TypedFieldId(MajorType type, int fieldId, boolean isHyper) {
    super();
    this.type = type;
    this.fieldId = fieldId;
    this.isHyperReader = isHyper;
  }

  public boolean isHyperReader(){
    return isHyperReader;
  }
  
  public MajorType getType() {
    return type;
  }

  public int getFieldId() {
    return fieldId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TypedFieldId other = (TypedFieldId) obj;
    if (fieldId != other.fieldId)
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "TypedFieldId [type=" + type + ", fieldId=" + fieldId + ", isSuperReader=" + isHyperReader + "]";
  }

  
}