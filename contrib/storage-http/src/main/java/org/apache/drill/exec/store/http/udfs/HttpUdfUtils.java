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

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.exec.store.http.HttpApiConfig;
import org.apache.drill.exec.store.http.HttpJsonOptions;
import org.apache.drill.exec.store.http.HttpStoragePlugin;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class HttpUdfUtils {

  private static final Logger logger = LoggerFactory.getLogger(HttpUdfUtils.class);

  public static JsonLoaderBuilder setupJsonBuilder(HttpApiConfig endpointConfig, ResultSetLoader loader, OptionManager options) {
    loader.setTargetRowCount(1);
    // Add JSON configuration from Storage plugin, if present.
    JsonLoaderBuilder jsonLoaderBuilder = new JsonLoaderBuilder()
      .resultSetLoader(loader)
      .maxRows(1)
      .standardOptions(options);

    // Add data path if present
    if (StringUtils.isNotEmpty(endpointConfig.dataPath())) {
      jsonLoaderBuilder.dataPath(endpointConfig.dataPath());
    }

    // Add JSON configuration from Storage plugin, if present.
    HttpJsonOptions jsonOptions = endpointConfig.jsonOptions();
    if (jsonOptions != null) {
      // Add options from endpoint configuration to jsonLoader
      JsonLoaderOptions jsonLoaderOptions = jsonOptions.getJsonOptions(options);
      jsonLoaderBuilder.options(jsonLoaderOptions);

      // Add provided schema if present
      if (jsonOptions.schema() != null) {
        logger.debug("Found schema: {}", jsonOptions.schema());
        jsonLoaderBuilder.providedSchema(jsonOptions.schema());
      }
    }
    loader.startBatch();
    return jsonLoaderBuilder;
  }

  public static void processResults(NullableVarCharHolder[] inputReaders,
                                    HttpStoragePlugin plugin,
                                    HttpApiConfig endpointConfig,
                                    DrillbitContext drillbitContext,
                                    JsonLoaderBuilder jsonLoaderBuilder,
                                    ResultSetLoader loader) {
    // Process Positional Arguments
    List args = SimpleHttp.buildParameterList(inputReaders);

    // If the arg list is null, indicating at least one null arg, return an empty map
    // as an approximation of null-if-null handling.
    if (args == null) {
      // Return empty map
      return;
    }

    InputStream results = SimpleHttp.apiCall(plugin, endpointConfig, drillbitContext, args)
      .getInputStream();

    // If the result string is null or empty, return an empty map
    if (results == null) {
      // Return empty map
      return;
    }

    try {
      String jsonInput = getStringFromInputStream(results);
      jsonLoaderBuilder.fromString(jsonInput);
      System.out.println("Batch size: {} " +  loader.maxBatchSize());
      JsonLoader jsonLoader = jsonLoaderBuilder.build();
      jsonLoader.readBatch();
      jsonLoader.close();
      loader.setTargetRowCount(loader.targetRowCount() + 1);

    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Error while reading JSON. ")
        .addContext(e.getMessage())
        .build(logger);
    }
  }

  /**
   * Reads an {@link InputStream} and returns the contents as a {@link String}.
   * @param in An {@link InputStream}
   * @return A {@link String} of the InputStream's contents
   */
  public static String getStringFromInputStream(InputStream in) {
    return new BufferedReader(
      new InputStreamReader(in, StandardCharsets.UTF_8))
      .lines()
      .collect(Collectors.joining("\n"));
  }
}
