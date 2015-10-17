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
package org.apache.drill.exec.expr.fn.impl.gis;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

import io.netty.buffer.DrillBuf;

@FunctionTemplate(name = "st_dwithin", scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STDWithin implements DrillSimpleFunc {
  @Param
  VarBinaryHolder geom1Param;

  @Param
  VarBinaryHolder geom2Param;

  @Param(constant = true)
  Float8Holder distanceParam;

  @Output
  BitHolder out;

  @Inject
  DrillBuf buffer;

  public void setup() {
  }

  public void eval() {
    double distance = distanceParam.value;

    com.esri.core.geometry.ogc.OGCGeometry geom1;
    com.esri.core.geometry.ogc.OGCGeometry geom2;

    geom1 = com.esri.core.geometry.ogc.OGCGeometry
        .fromBinary(geom1Param.buffer.nioBuffer(geom1Param.start, geom1Param.end - geom1Param.start));
    geom2 = com.esri.core.geometry.ogc.OGCGeometry
        .fromBinary(geom2Param.buffer.nioBuffer(geom2Param.start, geom2Param.end - geom2Param.start));

    int isWithin = geom1.distance(geom2) <= distance ? 1 : 0;

    out.value = isWithin;
  }
}
