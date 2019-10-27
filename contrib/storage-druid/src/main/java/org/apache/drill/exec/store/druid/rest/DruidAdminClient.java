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
package org.apache.drill.exec.store.druid.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.druid.druid.SimpleDatasourceInfo;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DruidAdminClient {
  private static final Logger logger = LoggerFactory.getLogger(DruidAdminClient.class);

  private static final String DATASOURCES_BASE_URI = "/druid/coordinator/v1/datasources?simple";
  private static final String DEFAULT_ENCODING = "UTF-8";
  private static final ObjectMapper mapper = new ObjectMapper();

  private final String coordinatorAddress;
  private final RestClient restClient;

  public DruidAdminClient(String coordinatorAddress, RestClient restClient) {
    this.coordinatorAddress = coordinatorAddress;
    this.restClient = restClient;
  }

  public List<SimpleDatasourceInfo> getDataSources() throws IOException {
    String url = this.coordinatorAddress + DATASOURCES_BASE_URI;
    HttpResponse response = restClient.get(url);

    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      throw UserException
        .dataReadError()
        .message("Error getting druid datasources. HTTP request failed")
        .addContext("Response code", response.getStatusLine().getStatusCode())
        .addContext("Response message", response.getStatusLine().getReasonPhrase())
        .build(logger);
    }

    String responseJson = EntityUtils.toString(response.getEntity(), DEFAULT_ENCODING);
    return mapper.readValue(responseJson, new TypeReference<List<SimpleDatasourceInfo>>(){});
  }
}
