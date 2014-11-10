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

package org.apache.drill.exec.expr.fn.impl.conv;

import io.netty.buffer.DrillBuf;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class JsonConvertTo {

 static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonConvertTo.class);

  private JsonConvertTo(){}

  @FunctionTemplate(name = "convert_toJSON", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ConvertToJson implements DrillSimpleFunc{

    @Param FieldReader input;
    @Output VarBinaryHolder out;
    @Inject DrillBuf buffer;

    public void setup(RecordBatch incoming){
    }

    public void eval(){
      out.start = 0;

      java.io.ByteArrayOutputStream stream = new java.io.ByteArrayOutputStream();
      try {
        org.apache.drill.exec.vector.complex.fn.JsonWriter jsonWriter = new org.apache.drill.exec.vector.complex.fn.JsonWriter(stream, true);

        jsonWriter.write(input);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      byte [] bytea = stream.toByteArray();

      out.buffer = buffer = buffer.reallocIfNeeded(bytea.length);
      out.buffer.setBytes(0, bytea);
      out.end = bytea.length;
    }
  }
}
