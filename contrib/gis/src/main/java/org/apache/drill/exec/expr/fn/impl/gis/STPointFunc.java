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
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

import io.netty.buffer.DrillBuf;

@FunctionTemplate(name = "st_point", scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STPointFunc implements DrillSimpleFunc {
  @Param
  Float8Holder lonParam;

  @Param
  Float8Holder latParam;

  @Output
  VarBinaryHolder out;

  @Inject
  DrillBuf buffer;

  public void setup() {
  }

  public void eval() {

    double lon = lonParam.value;
    double lat = latParam.value;

    com.esri.core.geometry.ogc.OGCPoint point = new com.esri.core.geometry.ogc.OGCPoint(
        new com.esri.core.geometry.Point(lon, lat), com.esri.core.geometry.SpatialReference.create(4326));

    java.nio.ByteBuffer pointBytes = point.asBinary();
    out.buffer = buffer;
    out.start = 0;
    out.end = pointBytes.remaining();
    buffer.setBytes(0, pointBytes);
  }
}
