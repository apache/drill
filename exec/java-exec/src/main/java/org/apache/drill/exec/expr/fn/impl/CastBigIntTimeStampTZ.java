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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.TimeStampTZHolder;
import org.apache.drill.exec.record.RecordBatch;

@SuppressWarnings("unused")
@FunctionTemplate(names = {"castTIMESTAMPTZ", "to_timestamptz"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls= NullHandling.NULL_IF_NULL)
public class CastBigIntTimeStampTZ implements DrillSimpleFunc {

    @Param
    BigIntHolder in;
    @Workspace
    int timeZoneIndex;
    @Output
    TimeStampTZHolder out;

    @Override
    public void setup(RecordBatch incoming) {
        org.joda.time.DateTime temp = new org.joda.time.DateTime();

        // Store the local time zone index
        timeZoneIndex = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(temp.getZone().toString());
    }

    @Override
    public void eval() {
        out.value = in.value;
        out.index = timeZoneIndex;
    }
}
