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

package org.apache.drill.exec.store.elasticsearch;

import java.io.InputStream;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.store.elasticsearch.schema.ElasticSearchTypeMappingLoader;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.TestCase;

// CSG Working

public class ElasticSearchTypeMappingsLoaderTest {

  @Mock
  private ElasticSearchStoragePlugin plugin;

  @Mock
  private RestClient restClient;

  @Before
  public void before() {
    ObjectMapper objectMapper = new ObjectMapper();
    restClient = Mockito.mock(RestClient.class);
    plugin = Mockito.mock(ElasticSearchStoragePlugin.class);
    Mockito.when(plugin.getClient()).thenReturn(restClient);
    Mockito.when(plugin.getObjectMapper()).thenReturn(objectMapper);
  }

  @Test
  public void getTypeMappingsOK() throws Exception {
    Response mockResponse = Mockito.mock(Response.class);
    HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
    InputStream responseContent = IOUtils.toInputStream("{\"employee\":{\"aliases\":{},\"mappings\":{\"developer\":{\"dynamic_templates\":[{\"disable_string_analyzing\":{\"match\":\"*\",\"match_mapping_type\":\"string\",\"mapping\":{\"index\":\"not_analyzed\",\"type\":\"string\"}}},{\"avoid_parsing_raws\":{\"match\":\"*Raw\",\"mapping\":{\"index\":\"not_analyzed\",\"type\":\"string\"}}}]}},\"settings\":{\"index\":{\"mapping\":{\"total_fields\":{\"limit\":\"10000\"}},\"number_of_shards\":\"11\",\"provided_name\":\"employee\",\"creation_date\":\"1491328427846\",\"number_of_replicas\":\"2\",\"uuid\":\"2WLTEDl6RAidfOPRsLwDZw\",\"version\":{\"created\":\"5030099\"}}}}}");
    try {
      Mockito.when(mockEntity.getContent()).thenReturn(responseContent);
      Mockito.when(restClient.performRequest("GET", "/" + "employee")).thenReturn(mockResponse);

      ElasticSearchTypeMappingLoader esil = new ElasticSearchTypeMappingLoader(plugin);
      Collection<String> load = esil.load("employee");
      TestCase.assertEquals(1, load.size());
      TestCase.assertTrue(load.contains("developer"));
    } finally {
      IOUtils.closeQuietly(responseContent);
    }
  }

  @Test
  public void getNoTypeMappingsOk() throws Exception {
    Response mockResponse = Mockito.mock(Response.class);
    HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
    InputStream responseContent = IOUtils.toInputStream("{\"" + ElasticSearchTestConstants.EMPLOYEE_IDX + "\":{\"aliases\":{},\"mappings\":{\"" + ElasticSearchConstants.DEFAULT_MAPPING + "\":{\"dynamic_templates\":[{\"disable_string_analyzing\":{\"match\":\"*\",\"match_mapping_type\":\"string\",\"mapping\":{\"index\":\"not_analyzed\",\"type\":\"string\"}}},{\"avoid_parsing_raws\":{\"match\":\"*Raw\",\"mapping\":{\"index\":\"not_analyzed\",\"type\":\"string\"}}}]}},\"settings\":{\"index\":{\"mapping\":{\"total_fields\":{\"limit\":\"10000\"}},\"number_of_shards\":\"11\",\"provided_name\":\"employee\",\"creation_date\":\"1491328427846\",\"number_of_replicas\":\"2\",\"uuid\":\"2WLTEDl6RAidfOPRsLwDZw\",\"version\":{\"created\":\"5030099\"}}}}}");
    try {
      Mockito.when(mockEntity.getContent()).thenReturn(responseContent);
      Mockito.when(restClient.performRequest("GET", "/" + "employee")).thenReturn(mockResponse);

      ElasticSearchTypeMappingLoader esil = new ElasticSearchTypeMappingLoader(this.plugin);
      Collection<String> load = esil.load("employee");
      TestCase.assertEquals(0, load.size());
    } finally {
      IOUtils.closeQuietly(responseContent);
    }
  }
}
