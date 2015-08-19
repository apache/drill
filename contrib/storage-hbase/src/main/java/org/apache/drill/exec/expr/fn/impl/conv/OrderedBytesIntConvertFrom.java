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

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

@FunctionTemplate(names = {"convert_fromINT_OB", "convert_fromINT_OBD"},
    scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class OrderedBytesIntConvertFrom implements DrillSimpleFunc {

  @Param VarBinaryHolder in;
  @Output IntHolder out;
  @Workspace byte[] bytes;
  @Workspace org.apache.hadoop.hbase.util.PositionedByteRange br;

  @Override
  public void setup() {
    bytes = new byte[5];
    br = new org.apache.hadoop.hbase.util.SimplePositionedByteRange();
  }

  @Override
  public void eval() {
    org.apache.drill.exec.util.ByteBufUtil.checkBufferLength(in.buffer, in.start, in.end, 5);
    in.buffer.getBytes(in.start, bytes, 0, 5);
    br.set(bytes);
    out.value = org.apache.hadoop.hbase.util.OrderedBytes.decodeInt32(br);
  }
}
