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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/hive/ObjectInspectorHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.hive;

import com.sun.codemodel.*;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.DirectExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.util.HashMap;
import java.util.Map;

public class ObjectInspectorHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ObjectInspectorHelper.class);

  private static Map<MinorType, Class> OIMAP = new HashMap<>();
  static {
<#list drillOI.map as entry>
    OIMAP.put(MinorType.${entry.minorType}, Drill${entry.holder}ObjectInspector.class);
</#list>
  }

  public static ObjectInspector getDrillObjectInspector(MinorType drillType) {
    if (OIMAP.containsKey(drillType)) {
      try {
        return (ObjectInspector)OIMAP.get(drillType).newInstance();
      } catch(InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Failed to instantiate ObjectInspector", e);
      }
    }

    throw new UnsupportedOperationException(drillType.toString());
  }

  public static JBlock initReturnValueHolder(JCodeModel m, JVar returnValueHolder, ObjectInspector oi, MinorType returnType) {
    JBlock block = new JBlock(false, false);
    switch(oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch(poi.getPrimitiveCategory()) {
<#list drillOI.map as entry>
          case ${entry.hiveType}:{
            JType holderClass = TypeHelper.getHolderType(m, returnType, TypeProtos.DataMode.OPTIONAL);
            block.assign(returnValueHolder, JExpr._new(holderClass));

          <#if entry.hiveType == "VARCHAR" || entry.hiveType == "STRING">
            block.assign(returnValueHolder.ref("buffer"),
              m.directClass(io.netty.buffer.Unpooled.class.getCanonicalName())
                .staticInvoke("wrappedBuffer")
                .arg(JExpr.newArray(m._ref(byte.class), JExpr.lit(1000))));
          </#if>
            return block;
          }
</#list>
          default:
            throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", poi.getPrimitiveCategory().toString()));
        }
      }

      case MAP:
      case LIST:
      case STRUCT:
      default:
        throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", oi.getCategory().toString()));
    }
  }

  private static Map<PrimitiveCategory, MinorType> TYPE_HIVE2DRILL = new HashMap<>();
  static {
<#list drillOI.map as entry>
    TYPE_HIVE2DRILL.put(PrimitiveCategory.${entry.hiveType}, MinorType.${entry.minorType});
</#list>
  }

  public static MinorType getDrillType(ObjectInspector oi) {
    switch(oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        if (TYPE_HIVE2DRILL.containsKey(poi.getPrimitiveCategory())) {
          return TYPE_HIVE2DRILL.get(poi.getPrimitiveCategory());
        }
        throw new UnsupportedOperationException();
      }

      case MAP:
      case LIST:
      case STRUCT:
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static JBlock getDrillObject(JCodeModel m, ObjectInspector oi,
    JVar returnOI, JVar returnValueHolder, JVar returnValue) {
    JBlock block = new JBlock(false, false);
    switch(oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch(poi.getPrimitiveCategory()) {
<#list drillOI.map as entry>
          case ${entry.hiveType}:{
            JConditional jc = block._if(returnValue.eq(JExpr._null()));
            jc._then().assign(returnValueHolder.ref("isSet"), JExpr.lit(0));
            jc._else().assign(returnValueHolder.ref("isSet"), JExpr.lit(1));
            JVar castedOI = jc._else().decl(
              m.directClass(${entry.hiveOI}.class.getCanonicalName()), "castOI", JExpr._null());
            jc._else().assign(castedOI,
              JExpr.cast(m.directClass(${entry.hiveOI}.class.getCanonicalName()), returnOI));

          <#if entry.hiveType == "BOOLEAN">
            JConditional booleanJC = jc._else()._if(castedOI.invoke("get").arg(returnValue));
            booleanJC._then().assign(returnValueHolder.ref("value"), JExpr.lit(1));
            booleanJC._else().assign(returnValueHolder.ref("value"), JExpr.lit(0));

          <#elseif entry.hiveType == "VARCHAR">
            JVar data = jc._else().decl(m.directClass(byte[].class.getCanonicalName()), "data",
              castedOI.invoke("getPrimitiveJavaObject").arg(returnValue)
                      .invoke("getValue")
                      .invoke("getBytes"));

            jc._else().add(returnValueHolder.ref("buffer")
              .invoke("setBytes").arg(JExpr.lit(0)).arg(data));


            jc._else().assign(returnValueHolder.ref("start"), JExpr.lit(0));
            jc._else().assign(returnValueHolder.ref("end"), data.ref("length"));

          <#elseif entry.hiveType == "STRING">
            JVar data = jc._else().decl(m.directClass(byte[].class.getCanonicalName()), "data",
              castedOI.invoke("getPrimitiveJavaObject").arg(returnValue)
                      .invoke("getBytes").arg(DirectExpression.direct("com.google.common.base.Charsets.UTF_16")));
            jc._else().add(returnValueHolder.ref("buffer")
              .invoke("setBytes").arg(JExpr.lit(0)).arg(data));
            jc._else().assign(returnValueHolder.ref("start"), JExpr.lit(0));
            jc._else().assign(returnValueHolder.ref("end"), data.ref("length"));

          <#else>
            jc._else().assign(returnValueHolder.ref("value"),
              castedOI.invoke("get").arg(returnValue));
          </#if>
            return block;
          }

</#list>
          default:
            throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", poi.getPrimitiveCategory().toString()));
        }
      }

      case MAP:
      case LIST:
      case STRUCT:
      default:
        throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", oi.getCategory().toString()));
    }
  }
}
