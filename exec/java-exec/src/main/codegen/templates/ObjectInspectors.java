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
<#list drillOI.map as entry>


<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/hive/Drill${entry.holder}ObjectInspector.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.hive;

import org.apache.drill.exec.expr.holders.*;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Text;

public class Drill${entry.holder}ObjectInspector extends AbstractPrimitiveObjectInspector
  implements ${entry.hiveOI} {

  @Override
  public String getTypeName() {
    return serdeConstants.${entry.serdeConstant};
  }

<#if entry.minorType == "VARCHAR">
  @Override
  public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    HiveVarcharWritable valW = new HiveVarcharWritable();
    valW.set(getPrimitiveJavaObject(o));
    return valW;
  }

  @Override
  public HiveVarchar getPrimitiveJavaObject(Object o) {
    String val = ((VarCharHolder)o).toString();
    return new HiveVarchar(val, HiveVarchar.MAX_VARCHAR_LENGTH);
  }
<#elseif entry.minorType == "VAR16CHAR">
@Override
  public Text getPrimitiveWritableObject(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
    if (o instanceof Var16CharHolder)
    return ((Var16CharHolder)o).toString();
    else
    return ((NullableVar16CharHolder)o).toString();
  }
<#elseif entry.minorType == "VARBINARY">  
@Override
public org.apache.hadoop.io.BytesWritable getPrimitiveWritableObject(Object o) {
  throw new UnsupportedOperationException();
}

@Override
public byte[] getPrimitiveJavaObject(Object o) {
  if (o instanceof VarBinaryHolder){
    VarBinaryHolder h = (VarBinaryHolder)o;
    byte[] buf = new byte[h.end-h.start];
    h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
    return buf;
  }else{
    NullableVarBinaryHolder h = (NullableVarBinaryHolder)o;
    byte[] buf = new byte[h.end-h.start];
    h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
    return buf;
    
  }
}
<#elseif entry.minorType == "BIT">
  @Override
  public boolean get(Object o) {
    if (o instanceof BitHolder)
    return ((BitHolder)o).value == 0 ? false : true;
    else
    return ((NullableBitHolder)o).value == 0 ? false : true;
  }
<#else>
  @Override
  public ${entry.javaType} get(Object o) {
    if (o instanceof ${entry.holder}Holder)
    return ((${entry.holder}Holder)o).value;
    else
    return ((Nullable${entry.holder}Holder)o).value;
  }
</#if>

  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return PrimitiveCategory.${entry.hiveType};
  }
}

</#list>

