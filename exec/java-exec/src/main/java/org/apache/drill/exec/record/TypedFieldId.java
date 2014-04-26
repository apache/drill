/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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