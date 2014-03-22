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
<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/TypeHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr;

<#include "/@includes/vv_imports.ftl" />
import org.apache.drill.exec.vector.accessor.*;

public class TypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  private static final int WIDTH_ESTIMATE = 50;

  public static int getSize(MajorType major) {
    switch (major.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case}:
      return ${type.width}<#if minor.class?substring(0, 3) == "Var" ||
                               minor.class?substring(0, 3) == "PRO" ||
                               minor.class?substring(0, 3) == "MSG"> + WIDTH_ESTIMATE</#if>;
  </#list>
</#list>
      case FIXEDCHAR: return major.getWidth();
      case FIXED16CHAR: return major.getWidth();
      case FIXEDBINARY: return major.getWidth();
    }
    throw new UnsupportedOperationException();
  }

  public static SqlAccessor getSqlAccessor(ValueVector vector){
    switch(vector.getField().getType().getMinorType()){
    <#list vv.types as type>
    <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (vector.getField().getType().getMode()) {
        case REQUIRED:
          return new ${minor.class}Accessor((${minor.class}Vector) vector);
        case OPTIONAL:
          return new Nullable${minor.class}Accessor((Nullable${minor.class}Vector) vector);
        case REPEATED:
          throw new UnsupportedOperationException();
      }
    </#list>
    </#list>
    }
    throw new UnsupportedOperationException();
  }
  
  public static Class<?> getValueVectorClass(MinorType type, DataMode mode){
    switch (type) {
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}Vector.class;
          case OPTIONAL:
            return Nullable${minor.class}Vector.class;
          case REPEATED:
            return Repeated${minor.class}Vector.class;
        }
  </#list>
</#list>
    default:
      break;
    }
    throw new UnsupportedOperationException();
  }

  public static JType getHolderType(JCodeModel model, MinorType type, DataMode mode){
    switch (type) {
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return model._ref(${minor.class}Holder.class);
          case OPTIONAL:
            return model._ref(Nullable${minor.class}Holder.class);
          case REPEATED:
            return model._ref(Repeated${minor.class}Holder.class);
        }
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
<#list vv.  types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (type.getMode()) {
        case REQUIRED:
          return new ${minor.class}Vector(field, allocator);
        case OPTIONAL:
          return new Nullable${minor.class}Vector(field, allocator);
        case REPEATED:
          return new Repeated${minor.class}Vector(field, allocator);
      }
  </#list>
</#list>
    default:
      break;
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(type.getMinorType() + " type is not supported. Mode: " + type.getMode());
  }

  public static ValueHolder getValue(ValueVector vector, int index) {
    MajorType type = vector.getField().getType();
    ValueHolder holder;
    switch(type.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case} :
      <#if minor.class?starts_with("Var")>
         throw new UnsupportedOperationException(type.getMinorType() + " type is not supported."); 
      <#else>
      holder = new ${minor.class}Holder(); 
      ((${minor.class}Holder)holder).value = ((${minor.class}Vector) vector).getAccessor().get(index);
      break;
      </#if>
  </#list>
</#list>
    default:
      throw new UnsupportedOperationException(type.getMinorType() + " type is not supported."); 
    }
    return holder;
  }

  public static void setValue(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case} :
      ((${minor.class}Vector) vector).getMutator().set(index, (${minor.class}Holder) holder);
      break;
  </#list>
</#list>
    default:
      throw new UnsupportedOperationException(type.getMinorType() + " type is not supported.");    
    }
  }

  public static boolean compareValues(ValueVector v1, int v1index, ValueVector v2, int v2index) {
    MajorType type1 = v1.getField().getType();
    MajorType type2 = v2.getField().getType();

    if (type1.getMinorType() != type2.getMinorType()) {
      return false;
    }

    switch(type1.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case} :
      if ( ((${minor.class}Vector) v1).getAccessor().get(v1index) == 
           ((${minor.class}Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
  </#list>
</#list>
    default:
      break;
    }
    return false;
  }

}
