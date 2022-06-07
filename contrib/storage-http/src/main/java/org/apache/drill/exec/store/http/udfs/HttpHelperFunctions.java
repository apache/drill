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

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
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

    @Output // todo: remove. Not used in this UDF
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader loader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder jsonLoaderBuilder;

    @Override
    public void setup() {
      jsonLoaderBuilder = new org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder()
        .resultSetLoader(loader)
        .standardOptions(options);
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
        return;
      }

      String finalUrl = org.apache.drill.exec.store.http.util.SimpleHttp.mapPositionalParameters(url, args);

      // Make the API call
      java.io.InputStream results = org.apache.drill.exec.store.http.util.SimpleHttp.getRequestAndStreamResponse(finalUrl);

      // If the result string is null or empty, return an empty map
      if (results == null) {
        // Return empty map
        return;
      }

      try {
        jsonLoaderBuilder.fromStream(results);
        org.apache.drill.exec.store.easy.json.loader.JsonLoader jsonLoader = jsonLoaderBuilder.build();
        loader.startBatch();
        jsonLoader.readBatch();
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

    @Output // todo: remove. Not used in this UDF
    ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    DrillbitContext drillbitContext;

    @Inject
    ResultSetLoader loader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder jsonLoaderBuilder;

    @Workspace
    org.apache.drill.exec.store.http.HttpStoragePlugin plugin;

    @Workspace
    org.apache.drill.exec.store.http.HttpApiConfig endpointConfig;

    @Override
    public void setup() {
      jsonLoaderBuilder = new org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder()
        .resultSetLoader(loader)
        .standardOptions(options);

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
        return;
      }

      java.io.InputStream results = org.apache.drill.exec.store.http.util.SimpleHttp.apiCall(
        plugin,
        endpointConfig,
        drillbitContext,
        args
      ).getInputStream();

      // If the result string is null or empty, return an empty map
      if (results == null) {
        // Return empty map
        return;
      }

      try {
        jsonLoaderBuilder.fromStream(results);
        org.apache.drill.exec.store.easy.json.loader.JsonLoader jsonLoader = jsonLoaderBuilder.build();
        loader.startBatch();
        jsonLoader.readBatch();
      } catch (Exception e) {
        throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }
}
