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
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

public class ConvertFromImpalaTimestamp {


  @FunctionTemplate(name = "convert_fromTIMESTAMP_IMPALA", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ImpalaTimestampConvertFrom implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Output TimeStampHolder out;


    @Override
    public void setup() { }

    @Override
    public void eval() {
      org.apache.drill.exec.util.ByteBufUtil.checkBufferLength(in.buffer, in.start, in.end, 12);

      in.buffer.readerIndex(in.start);
      long nanosOfDay = in.buffer.readLong();
      int julianDay = in.buffer.readInt();
      /* We use the same implementation as org.joda.time.DateTimeUtils.fromJulianDay but avoid rounding errors
         Note we need to subtract half of a day because julian days are recorded as starting at noon.
         From Joda :
              public static final long fromJulianDay(double julianDay) {
                484            double epochDay = julianDay - 2440587.5d;
                485            return (long) (epochDay * 86400000d);
                486        }
      */
      long dateTime = (julianDay - 2440588)*86400000L + (nanosOfDay / 1000000);
      out.value = new org.joda.time.DateTime((long) dateTime, org.joda.time.chrono.JulianChronology.getInstance()).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
    }
  }
}
