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

import okhttp3.HttpUrl;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.http.HttpApiConfig;
import org.apache.drill.exec.store.http.HttpStoragePlugin;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.apache.drill.exec.store.http.util.HttpProxyConfig;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.store.http.util.SimpleHttp.SimpleHttpBuilder;
import org.apache.drill.exec.util.HttpUtils;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class HttpHelperUtils {

  private static Logger logger = LoggerFactory.getLogger(HttpHelperUtils.class);
  /**
   * Accepts a list of input readers and converts that into an ArrayList of Strings
   * @param inputReaders The array of FieldReaders
   * @return A List of Strings containing the values from the FieldReaders.
   */
  public static List<String> buildParameterList(FieldReader[] inputReaders) {
    List<String> inputArguments = new ArrayList<>();

    // Skip the first argument because that is the input URL
    for (int i = 1; i < inputReaders.length; i++) {
      inputArguments.add(inputReaders[i].readObject().toString());
    }

    return inputArguments;
  }

  public static HttpStoragePluginConfig getPluginConfig(String name, DrillbitContext context) throws PluginException {
    HttpStoragePlugin httpStoragePlugin = getStoragePlugin(context, name);
    return httpStoragePlugin.getConfig();
  }

  public static HttpApiConfig getEndpointConfig(String name, DrillbitContext context) {
    // Get the plugin name and endpoint name
    String[] parts = name.split("\\.");
    if (parts.length < 2) {
      throw UserException.functionError()
        .message("You must call this function with a connection name and endpoint.")
        .build(logger);
    }
    String plugin = parts[0];
    String endpoint = parts[1];

    HttpStoragePlugin httpStoragePlugin = getStoragePlugin(context, plugin);
    HttpStoragePluginConfig config = httpStoragePlugin.getConfig();

    HttpApiConfig endpointConfig = config.getConnection(endpoint);
    if (endpointConfig == null) {
      throw UserException.functionError()
        .message("You must call this function with a valid endpoint name.")
        .build(logger);
    } else if (endpointConfig.inputType() != "json") {
      throw UserException.functionError()
        .message("Http_get only supports API endpoints which return json.")
        .build(logger);
    }

    return endpointConfig;
  }

  private static HttpStoragePlugin getStoragePlugin(DrillbitContext context, String pluginName) {
    StoragePluginRegistry storage = context.getStorage();
    try {
      StoragePlugin pluginInstance = storage.getPlugin(pluginName);
      if (pluginInstance == null) {
        throw UserException.functionError()
          .message(pluginName + " is not a valid plugin.")
          .build(logger);
      }

      if (!(pluginInstance instanceof HttpStoragePlugin)) {
        throw UserException.functionError()
          .message("You can only include HTTP plugins in this function.")
          .build(logger);
      }
      return (HttpStoragePlugin) pluginInstance;
    } catch (PluginException e) {
      throw UserException.functionError()
        .message("Could not access plugin " + pluginName)
        .build(logger);
    }
  }


  /**
   * This function makes an API call and returns a string of the parsed results. It is used in the http_get() UDF
   * and retrieves all the configuration parameters contained in the storage plugin and endpoint configuration. The exception
   * is pagination.  This does not support pagination.
   * @param schemaPath The path of storage_plugin.endpoint from which the data will be retrieved
   * @param context {@link DrillbitContext} The context from the current query
   * @param args An optional list of parameter arguments which will be included in the URL
   * @return A String of the results.
   */
  public static String makeAPICall(String schemaPath, DrillbitContext context, List<String> args) {
    HttpStoragePluginConfig pluginConfig;
    HttpApiConfig endpointConfig;

    // Get the plugin name and endpoint name
    String[] parts = schemaPath.split("\\.");
    if (parts.length < 2) {
      throw UserException.functionError()
        .message("You must call this function with a connection name and endpoint.")
        .build(logger);
    }
    String pluginName = parts[0];

    HttpStoragePlugin plugin = getStoragePlugin(context, pluginName);

    try {
      pluginConfig = getPluginConfig(pluginName, context);
      endpointConfig = getEndpointConfig(schemaPath, context);
    } catch (PluginException e) {
      throw UserException.functionError()
        .message("Could not access plugin " + pluginName)
        .build(logger);
    }

    // Get proxy settings
    HttpProxyConfig proxyConfig = SimpleHttp.getProxySettings(pluginConfig, context.getConfig(), endpointConfig.getHttpUrl());

    // For this use case, we will replace the URL parameters here, rather than doing it in the SimpleHttp client
    // because we are using positional mapping rather than k/v pairs for this.
    String finalUrl;
    if (SimpleHttp.hasURLParameters(endpointConfig.getHttpUrl())) {
      finalUrl = HttpUtils.mapPositionalParameters(endpointConfig.url(), args);
    } else {
      finalUrl = endpointConfig.url();
    }

    // Now get the client
    SimpleHttp client = new SimpleHttpBuilder()
      .pluginConfig(pluginConfig)
      .endpointConfig(endpointConfig)
      .tempDir(new File(context.getConfig().getString(ExecConstants.DRILL_TMP_DIR)))
      .url(HttpUrl.parse(finalUrl))
      .proxyConfig(proxyConfig)
      .tokenTable(plugin.getTokenTable())
      .build();

    return client.getResultsFromApiCall();
  }


}
