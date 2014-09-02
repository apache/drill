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


import java.io.ByteArrayOutputStream;
import java.io.StringReader;

import javax.inject.Inject;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.google.common.base.Charsets;

public class JsonConvertFrom {

 static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonConvertFrom.class);

  private JsonConvertFrom(){}

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, isRandom = true)
  public static class ConvertFromJson implements DrillSimpleFunc{

    @Param VarBinaryHolder in;
    @Inject DrillBuf buffer;

    @Output ComplexWriter writer;

    public void setup(RecordBatch incoming){
    }

    public void eval(){

      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf, com.google.common.base.Charsets.UTF_8);

      try {
        org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader(buffer, false);

        jsonReader.write(new java.io.StringReader(input), writer);

      } catch (Exception e) {
//        System.out.println("Error while converting from JSON. ");
//        e.printStackTrace();
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, isRandom = true)
  public static class ConvertFromJsonVarchar implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output ComplexWriter writer;
    @Inject DrillBuf buffer;

    public void setup(RecordBatch incoming){
    }

    public void eval(){

      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf, com.google.common.base.Charsets.UTF_8);

      try {
        org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader(buffer, false);

        jsonReader.write(new java.io.StringReader(input), writer);

      } catch (Exception e) {
//        System.out.println("Error while converting from JSON. ");
//        e.printStackTrace();
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

}
