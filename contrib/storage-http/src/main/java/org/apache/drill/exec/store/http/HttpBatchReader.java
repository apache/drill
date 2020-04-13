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
package org.apache.drill.exec.store.http;

import java.io.File;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.http.util.HttpProxyConfig;
import org.apache.drill.exec.store.http.util.HttpProxyConfig.ProxyBuilder;
import org.apache.drill.exec.store.http.util.SimpleHttp;

import com.typesafe.config.Config;

public class HttpBatchReader implements ManagedReader<SchemaNegotiator> {
  private final HttpStoragePluginConfig config;
  private final HttpSubScan subScan;
  private JsonLoader jsonLoader;

  public HttpBatchReader(HttpStoragePluginConfig config, HttpSubScan subScan) {
    this.config = config;
    this.subScan = subScan;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    CustomErrorContext errorContext = negotiator.parentErrorContext();

    // Result set loader setup
    String tempDirPath = negotiator
        .drillConfig()
        .getString(ExecConstants.DRILL_TMP_DIR);
    ResultSetLoader loader = negotiator.build();

    // Http client setup
    SimpleHttp http = new SimpleHttp(config, new File(tempDirPath), subScan.tableSpec().database(), proxySettings(negotiator.drillConfig()), errorContext);

    // JSON loader setup
    jsonLoader = new JsonLoaderBuilder()
        .resultSetLoader(loader)
        .standardOptions(negotiator.queryOptions())
        .errorContext(errorContext)
        .fromStream(http.getInputStream(subScan.getFullURL()))
        .build();

    // Please read the first batch
    return true;
  }

 private HttpProxyConfig proxySettings(Config drillConfig) {
    ProxyBuilder builder = HttpProxyConfig.builder()
        .fromConfigForURL(drillConfig, subScan.getFullURL());
    String proxyType = config.proxyType();
    if (proxyType != null && !"direct".equals(proxyType)) {
      builder
        .type(config.proxyType())
        .host(config.proxyHost())
        .port(config.proxyPort())
        .username(config.proxyUsername())
        .password(config.proxyPassword());
    }
    return builder.build();
  }

 @Override
  public boolean next() {
    return jsonLoader.readBatch();
  }

  @Override
  public void close() {
    if (jsonLoader != null) {
      jsonLoader.close();
      jsonLoader = null;
    }
  }
}
