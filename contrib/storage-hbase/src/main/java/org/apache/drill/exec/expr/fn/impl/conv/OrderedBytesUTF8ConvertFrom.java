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
package org.apache.drill.exec.expr.fn.impl.conv;

import javax.inject.Inject;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

@FunctionTemplate(names = {"convert_fromUTF8_OB", "convert_fromUTF8_OBD"},
    scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class OrderedBytesUTF8ConvertFrom implements DrillSimpleFunc {

  @Param VarBinaryHolder in;
  @Output VarCharHolder out;
  @Inject DrillBuf buffer;

  @Override
  public void setup() { }

  @Override
  public void eval() {
    buffer = buffer.reallocIfNeeded(in.end - in.start - 2);
    byte[] bytes = new byte[in.end - in.start];
    in.buffer.getBytes(in.start, bytes, 0, in.end - in.start);
    org.apache.hadoop.hbase.util.PositionedByteRange br =
      new org.apache.hadoop.hbase.util.SimplePositionedByteRange(bytes, 0, in.end - in.start);
    String str = org.apache.hadoop.hbase.util.OrderedBytes.decodeString(br);
    buffer.setBytes(0,  str.getBytes(), 0, str.length());
    out.buffer = buffer;
    out.start =  0;
    out.end = str.length();
  }
}