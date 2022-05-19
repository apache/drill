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

package org.apache.drill.exec.store.http.udfs;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import javax.inject.Inject;

public class HttpHelperFunctions {

  @FunctionTemplate(names = {"http_get", "httpGet"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    isVarArg = true)
  public static class HttpGetFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder rawInput;

    @Param
    NullableVarCharHolder[] inputReaders;

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Override
    public void setup() {
      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
        .defaultSchemaPathColumns()
        .readNumbersAsDouble(options.getOption(org.apache.drill.exec.ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE).bool_val)
        .allTextMode(options.getOption(org.apache.drill.exec.ExecConstants.JSON_ALL_TEXT_MODE).bool_val)
        .enableNanInf(options.getOption(org.apache.drill.exec.ExecConstants.JSON_READER_NAN_INF_NUMBERS).bool_val)
        .build();

      jsonReader.setIgnoreJSONParseErrors(
        options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_READER_SKIP_INVALID_RECORDS_FLAG));
    }

    @Override
    public void eval() {
      // Get the URL
      String url = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawInput.start, rawInput.end, rawInput.buffer);

      // Process Positional Arguments
      java.util.List args = org.apache.drill.exec.store.http.util.SimpleHttp.buildParameterList(inputReaders);
      // If the arg list is null, indicating at least one null arg, return an empty map
      // as an approximation of null-if-null handling.
      if (args == null) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      String finalUrl = org.apache.drill.exec.store.http.util.SimpleHttp.mapPositionalParameters(url, args);

      // Make the API call
      String results = org.apache.drill.exec.store.http.util.SimpleHttp.makeSimpleGetRequest(finalUrl);

      // If the result string is null or empty, return an empty map
      if (results == null || results.length() == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(results);
        jsonReader.setIgnoreJSONParseErrors(true);  // Reduce number of errors
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }


  @FunctionTemplate(names = {"http_request", "httpRequest"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    isVarArg = true)
  public static class HttpGetFromStoragePluginFunction implements DrillSimpleFunc {

    @Param(constant = true)
    VarCharHolder rawInput;

    @Param
    NullableVarCharHolder[] inputReaders;

    @Output
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    DrillbitContext drillbitContext;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Workspace
    org.apache.drill.exec.store.http.HttpStoragePlugin plugin;

    @Workspace
    org.apache.drill.exec.store.http.HttpApiConfig endpointConfig;

    @Override
    public void setup() {
      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
        .defaultSchemaPathColumns()
        .readNumbersAsDouble(options.getOption(org.apache.drill.exec.ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE).bool_val)
        .allTextMode(options.getOption(org.apache.drill.exec.ExecConstants.JSON_ALL_TEXT_MODE).bool_val)
        .enableNanInf(options.getOption(org.apache.drill.exec.ExecConstants.JSON_READER_NAN_INF_NUMBERS).bool_val)
        .build();

      jsonReader.setIgnoreJSONParseErrors(
        options.getBoolean(org.apache.drill.exec.ExecConstants.JSON_READER_SKIP_INVALID_RECORDS_FLAG));

      String schemaPath = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawInput.start, rawInput.end, rawInput.buffer);
      // Get the plugin name and endpoint name
      String[] parts = schemaPath.split("\\.");
      if (parts.length < 2) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException(
          "You must call this function with a connection name and endpoint."
        );
      }
      String pluginName = parts[0], endpointName = parts[1];

      plugin = org.apache.drill.exec.store.http.util.SimpleHttp.getStoragePlugin(
        drillbitContext,
        pluginName
      );
      endpointConfig = org.apache.drill.exec.store.http.util.SimpleHttp.getEndpointConfig(
        endpointName,
        plugin.getConfig()
      );
    }

    @Override
    public void eval() {
      // Process Positional Arguments
      java.util.List args = org.apache.drill.exec.store.http.util.SimpleHttp.buildParameterList(inputReaders);
      // If the arg list is null, indicating at least one null arg, return an empty map
      // as an approximation of null-if-null handling.
      if (args == null) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      String results = org.apache.drill.exec.store.http.util.SimpleHttp.makeAPICall(
        plugin,
        endpointConfig,
        drillbitContext,
        args
      );

      // If the result string is null or empty, return an empty map
      if (results == null || results.length() == 0) {
        // Return empty map
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(results);
        jsonReader.setIgnoreJSONParseErrors(true);  // Reduce number of errors
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }
}
