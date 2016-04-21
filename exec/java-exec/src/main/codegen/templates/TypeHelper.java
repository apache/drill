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

import org.apache.drill.exec.vector.complex.UnionVector;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/TypeHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr;

<#include "/@includes/vv_imports.ftl" />
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.vector.accessor.*;

public class TypeHelper extends BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  public static SqlAccessor getSqlAccessor(ValueVector vector){
    final MajorType type = vector.getField().getType();
    switch(type.getMinorType()){
    case UNION:
      return new UnionSqlAccessor((UnionVector) vector);
    <#list vv.types as type>
    <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (type.getMode()) {
        case REQUIRED:
          return new ${minor.class}Accessor((${minor.class}Vector) vector);
        case OPTIONAL:
          return new Nullable${minor.class}Accessor((Nullable${minor.class}Vector) vector);
        case REPEATED:
          return new GenericAccessor(vector);
      }
    </#list>
    </#list>
    case MAP:
    case LIST:
      return new GenericAccessor(vector);
    }
    throw new UnsupportedOperationException(buildErrorMessage("find sql accessor", (type)));
  }
  
  public static JType getHolderType(JCodeModel model, MinorType type, DataMode mode){
    switch (type) {
    case UNION:
      return model._ref(UnionHolder.class);
    case MAP:
    case LIST:
      return model._ref(ComplexHolder.class);
      
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
      case GENERIC_OBJECT:
        return model._ref(ObjectHolder.class);
      default:
        break;
      }
    throw new UnsupportedOperationException(buildErrorMessage("get holder type", type,mode));
  }

  public static void load(ValueVector v, SerializedField metadata, ArrowBuf buffer) {
    MinorType type = v.getField().getType().getMinorType();
    DataMode mode = v.getField().getType().getMode();
    switch(type) {
    case LATE:
      new ZeroVectorHelper((ZeroVector) v).load(metadata, buffer);
      return;
    case UNION:
      new UnionVectorHelper((UnionVector) v).load(metadata, buffer);
      return;
    case LIST:
      switch(mode) {
      case REQUIRED:
      case OPTIONAL:
        new ListVectorHelper((ListVector) v).load(metadata, buffer);
        return;
      case REPEATED:
        new RepeatedListVectorHelper((RepeatedListVector) v).load(metadata, buffer);
        return;
      }
    case MAP:
      switch(mode) {
      case REQUIRED:
      case OPTIONAL:
        new MapVectorHelper((MapVector) v).load(metadata, buffer);
        return;
      case REPEATED:
        new RepeatedMapVectorHelper((RepeatedMapVector) v).load(metadata, buffer);
        return;
      }
    <#list vv.types as type>
    <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (mode) {
      case REQUIRED:
        new ${minor.class}VectorHelper((${minor.class}Vector) v).load(metadata, buffer);
        return;
      case OPTIONAL:
        new Nullable${minor.class}VectorHelper((Nullable${minor.class}Vector) v).load(metadata, buffer);
        return;
      case REPEATED:
        new Repeated${minor.class}VectorHelper((Repeated${minor.class}Vector) v).load(metadata, buffer);
        return;
      }
    </#list>
    </#list>
    }
  }

  public static SerializedField.Builder getMetadataBuilder(ValueVector v) {
    MinorType type = v.getField().getType().getMinorType();
    DataMode mode = v.getField().getType().getMode();
    switch(type) {
    case UNION:
      break;
    case LIST:
      switch(mode) {
      case REQUIRED:
      case OPTIONAL:
        return new ListVectorHelper((ListVector) v).getMetadataBuilder();
      case REPEATED:
        break;
      }
      break;
    case MAP:
      break;
    <#list vv.types as type>
    <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (mode) {
      case REQUIRED:
        return new ${minor.class}VectorHelper((${minor.class}Vector) v).getMetadataBuilder();
      case OPTIONAL:
        return new Nullable${minor.class}VectorHelper((Nullable${minor.class}Vector) v).getMetadataBuilder();
      case REPEATED:
        return new Repeated${minor.class}VectorHelper((Repeated${minor.class}Vector) v).getMetadataBuilder();
      }
    </#list>
    </#list>
    }
    return null;
  }

  public static SerializedField getMetadata(ValueVector v) {
    MinorType type = v.getField().getType().getMinorType();
    DataMode mode = v.getField().getType().getMode();
    switch(type) {
    case LATE:
      return new ZeroVectorHelper((ZeroVector) v).getMetadata();
    case UNION:
      return new UnionVectorHelper((UnionVector) v).getMetadata();
    case LIST:
      switch(mode) {
      case REQUIRED:
      case OPTIONAL:
        return new ListVectorHelper((ListVector) v).getMetadata();
      case REPEATED:
        return new RepeatedListVectorHelper((RepeatedListVector) v).getMetadata();
      }
    case MAP:
      switch(mode) {
      case REQUIRED:
      case OPTIONAL:
        return new MapVectorHelper((MapVector) v).getMetadata();
      case REPEATED:
        return new RepeatedMapVectorHelper((RepeatedMapVector) v).getMetadata();
      }
    <#list vv.types as type>
    <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (mode) {
      case REQUIRED:
        return new ${minor.class}VectorHelper((${minor.class}Vector) v).getMetadata();
      case OPTIONAL:
        return new Nullable${minor.class}VectorHelper((Nullable${minor.class}Vector) v).getMetadata();
      case REPEATED:
        return new Repeated${minor.class}VectorHelper((Repeated${minor.class}Vector) v).getMetadata();
      }
    </#list>
    </#list>
    }
    return null;
  }
}
