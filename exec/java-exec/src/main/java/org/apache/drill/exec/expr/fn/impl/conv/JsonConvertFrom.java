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
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import javax.inject.Inject;

@SuppressWarnings("unused")
public class JsonConvertFrom {

  private JsonConvertFrom() {
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJson implements DrillSimpleFunc {

    @Param
    VarBinaryHolder in;

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, 1, in.start, in.end, in.buffer, false, false, false);
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

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, 1, in.start, in.end, in.buffer, true,
        allTextModeHolder.value == 1, readNumbersAsDoubleHolder.value == 1);
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarchar implements DrillSimpleFunc {

    @Param
    VarCharHolder in;

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, 1, in.start, in.end, in.buffer, false, false, false);
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

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, 1, in.start, in.end, in.buffer, true,
        allTextModeHolder.value == 1, readNumbersAsDoubleHolder.value == 1);
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, in.isSet, in.start, in.end, in.buffer, false, false, false);
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

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, in.isSet, in.start, in.end, in.buffer, true,
        allTextModeHolder.value == 1, readNumbersAsDoubleHolder.value == 1);
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarcharNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder in;

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, in.isSet, in.start, in.end, in.buffer, false, false, false);
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

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.convertJson(
        rsLoader, jsonLoader, options, streamIter, in.isSet, in.start, in.end, in.buffer, true,
        allTextModeHolder.value == 1, readNumbersAsDoubleHolder.value == 1);
    }
  }

}
