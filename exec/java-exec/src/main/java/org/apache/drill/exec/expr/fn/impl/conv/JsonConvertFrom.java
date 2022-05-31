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


import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

public class JsonConvertFrom {

  private JsonConvertFrom() {}

  @FunctionTemplate(names = {"convert_fromJSON", "convertFromJson", "convert_from_json"},
    scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConvertFromJsonNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder jsonLoaderBuilder;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader loader;

    @Output
    BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {
      jsonLoaderBuilder = new org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder()
        .resultSetLoader(loader)
        .standardOptions(options);
    }

    @Override
    public void eval() {
      if (in.end == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonLoaderBuilder.fromStream(in.start, in.end, in.buffer);
        org.apache.drill.exec.store.easy.json.loader.JsonLoader jsonLoader = jsonLoaderBuilder.build();
        loader.startBatch();
        jsonLoader.readBatch();
        loader.close();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(names = {"convert_fromJSON", "convertFromJson", "convert_from_json"},
    scope = FunctionScope.SIMPLE)
  public static class ConvertFromJsonVarcharInput implements DrillSimpleFunc {

    @Param
    VarCharHolder in;

    @Output
    ComplexWriter writer;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder jsonLoaderBuilder;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader loader;

    @Override
    public void setup() {
     jsonLoaderBuilder = new org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder()
        .resultSetLoader(loader)
        .standardOptions(options);
    }

    @Override
    public void eval() {
      String jsonString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);

      // If the input is null or empty, return an empty map
      if (jsonString.length() == 0) {
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonLoaderBuilder.fromString(jsonString);
        org.apache.drill.exec.store.easy.json.loader.JsonLoader jsonLoader = jsonLoaderBuilder.build();
        loader.startBatch();
        jsonLoader.readBatch();
        loader.close();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }
}
