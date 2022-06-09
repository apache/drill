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
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.exec.store.http.HttpApiConfig;
import org.apache.drill.exec.store.http.HttpJsonOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUdfUtils {

  private static final Logger logger = LoggerFactory.getLogger(HttpUdfUtils.class);

  public static JsonLoaderBuilder setupJsonBuilder(HttpApiConfig endpointConfig, ResultSetLoader loader, OptionManager options) {
    loader.setTargetRowCount(1);
    // Add JSON configuration from Storage plugin, if present.
    HttpJsonOptions jsonOptions = endpointConfig.jsonOptions();
    JsonLoaderBuilder jsonLoaderBuilder = new JsonLoaderBuilder()
      .resultSetLoader(loader)
      .maxRows(1)
      .standardOptions(options);

    // Add data path if present
    if (StringUtils.isNotEmpty(endpointConfig.dataPath())) {
      jsonLoaderBuilder.dataPath(endpointConfig.dataPath());
    }

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
    return jsonLoaderBuilder;
  }
}
