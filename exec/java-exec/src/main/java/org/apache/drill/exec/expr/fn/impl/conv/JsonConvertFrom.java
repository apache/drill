/*
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
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import javax.inject.Inject;

public class JsonConvertFrom {

  private JsonConvertFrom() {
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJson implements DrillSimpleFunc {

    @Param
    VarBinaryHolder in;

    @Inject
    DrillBuf buffer;

    @Inject
    OptionManager options;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_ALL_TEXT_MODE);
      boolean readNumbersAsDouble = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonWithArgs implements DrillSimpleFunc {

    @Param
    VarBinaryHolder in;

    @Param
    BitHolder allTextModeHolder;

    @Param
    BitHolder readNumbersAsDoubleHolder;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = allTextModeHolder.value == 1;
      boolean readNumbersAsDouble = readNumbersAsDoubleHolder.value == 1;

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }


  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarchar implements DrillSimpleFunc {

    @Param
    VarCharHolder in;

    @Inject
    DrillBuf buffer;

    @Inject
    OptionManager options;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_ALL_TEXT_MODE);
      boolean readNumbersAsDouble = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarcharWithConfig implements DrillSimpleFunc {

    @Param
    VarCharHolder in;

    @Param
    BitHolder allTextModeHolder;

    @Param
    BitHolder readNumbersAsDoubleHolder;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = allTextModeHolder.value == 1;
      boolean readNumbersAsDouble = readNumbersAsDoubleHolder.value == 1;

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;

    @Inject
    DrillBuf buffer;

    @Inject
    OptionManager options;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_ALL_TEXT_MODE);
      boolean readNumbersAsDouble = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonNullableInputWithArgs implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;

    @Param
    BitHolder allTextModeHolder;

    @Param
    BitHolder readNumbersAsDoubleHolder;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = allTextModeHolder.value == 1;
      boolean readNumbersAsDouble = readNumbersAsDoubleHolder.value == 1;

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarcharNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder in;

    @Inject
    DrillBuf buffer;

    @Inject
    OptionManager options;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_ALL_TEXT_MODE);
      boolean readNumbersAsDouble = options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarcharNullableInputWithConfigs implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder in;

    @Param
    BitHolder allTextModeHolder;

    @Param
    BitHolder readNumbersAsDoubleHolder;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Output ComplexWriter writer;

    @Override
    public void setup() {
      boolean allTextMode = allTextModeHolder.value == 1;
      boolean readNumbersAsDouble = readNumbersAsDoubleHolder.value == 1;

      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .allTextMode(allTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .build();
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

}
