/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

public class TypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  private static final int WIDTH_ESTIMATE_1 = 10;
  private static final int WIDTH_ESTIMATE_2 = 50000;
  private static final int WIDTH_ESTIMATE_4 = 1024*1024;

  public static int getSize(MajorType major) {
    switch (major.getMinorType()) {
<#list types as type>
  <#list type.minor as minor>
    <#if minor.class != "Bit">
      case ${minor.class?upper_case}:
        return ${type.width}<#if minor.class?substring(0, 3) == "Var" ||
                                 minor.class?substring(0, 3) == "PRO" ||
                                 minor.class?substring(0, 3) == "MSG"> + WIDTH_ESTIMATE_${type.width}</#if>;
    </#if>
  </#list>
</#list>
      case BOOLEAN: return 1;
      case FIXEDCHAR: return major.getWidth();
      case FIXEDBINARY: return major.getWidth();
    }
    throw new UnsupportedOperationException();
  }

  public static Class<?> getValueVectorClass(MinorType type, DataMode mode){
    switch (type) {
<#list types as type>
  <#list type.minor as minor>
    <#if minor.class == "Bit">
      case BOOLEAN:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}Vector.class;
          case OPTIONAL:
            return Nullable${minor.class}Vector.class;
          case REPEATED:
            return Repeated${minor.class}Vector.class;
        }
    <#else>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}Vector.class;
          case OPTIONAL:
            return Nullable${minor.class}Vector.class;
          case REPEATED:
            return Repeated${minor.class}Vector.class;
        }
    </#if>
  </#list>
</#list>
    default:
      break;
    }
    throw new UnsupportedOperationException();
  }


  public static ValueVector getNewVector(MaterializedField field, BufferAllocator allocator){
    MajorType type = field.getType();

    switch (type.getMinorType()) {
<#list types as type>
  <#list type.minor as minor>
    <#if minor.class != "Bit">
      case ${minor.class?upper_case}:
        switch (type.getMode()) {
          case REQUIRED:
            return new ${minor.class}Vector(field, allocator);
          case OPTIONAL:
            return new Nullable${minor.class}Vector(field, allocator);
          case REPEATED:
            return new Repeated${minor.class}Vector(field, allocator);
        }
    </#if>
  </#list>
</#list>
      case BOOLEAN:
        switch (type.getMode()) {
          case REQUIRED:
            return new BitVector(field, allocator);
          case OPTIONAL:
            return new NullableBitVector(field, allocator);
          case REPEATED:
            return new RepeatedBitVector(field, allocator);
        }
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(type.getMinorType() + " type is not supported. Mode: " + type.getMode());
  }

}
