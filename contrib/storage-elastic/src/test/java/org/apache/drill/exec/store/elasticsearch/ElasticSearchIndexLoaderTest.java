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

import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.store.elasticsearch.schema.ElasticSearchIndexLoader;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.Collection;

public class ElasticSearchIndexLoaderTest {

    @Mock
    private ElasticSearchStoragePlugin plugin;
    @Mock
    private RestClient restClient;

    @Before
    public void before() {
        ObjectMapper objectMapper = new ObjectMapper();
        this.restClient = Mockito.mock(RestClient.class);
        this.plugin = Mockito.mock(ElasticSearchStoragePlugin.class);
        Mockito.when(plugin.getClient()).thenReturn(this.restClient);
        Mockito.when(plugin.getObjectMapper()).thenReturn(objectMapper);
    }

    /*
    This test verifies if ES is getting aliases for search indexes.  Data is formatted as follows:
    {
      "collection-file-info-v00001": {
        "aliases": {
          "collection-file-info": {},
          "collection-file-info-1": {}
        }
      },
      "collection-file-info-v00002": {
        "aliases": {
          "collection-file-info-2": {},
          "collection-file-info-3": {}
        }
      }
    }
    */
    @Test
    public void getAliasesOk() throws Exception {
        Response mockResponse = Mockito.mock(Response.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
        String jsonData = "{\"collection-file-info-v00001\":{\"aliases\":{\"collection-file-info\":{},\"collection-file-info-1\":{}}}," +
          "\"collection-file-info-v00002\":{\"aliases\":{\"collection-file-info-2\":{},\"collection-file-info-3\":{}}}}";
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        InputStream responseContent = IOUtils.toInputStream(jsonData);
        try {
            Mockito.when(mockEntity.getContent()).thenReturn(responseContent);
            Mockito.when(restClient.performRequest(new Request("GET", "/_aliases")))
              .thenReturn(mockResponse);

            ElasticSearchIndexLoader esil = new ElasticSearchIndexLoader(this.plugin);
            Collection<String> load = esil.load(ElasticSearchConstants.INDEXES);
            System.out.println(load.toString());
            TestCase.assertEquals(4, load.size());
            TestCase.assertTrue(load.contains("collection-file-info"));
            TestCase.assertTrue(load.contains("collection-file-info-1"));
            TestCase.assertTrue(load.contains("collection-file-info-2"));
            TestCase.assertTrue(load.contains("collection-file-info-3"));
        } finally {
            IOUtils.closeQuietly(responseContent);
        }
    }

    /*
    {
        "twitter":{},
        "collection": {
            "aliases":{ "collection-1":{},
                        "collection-2":{}
                      }
         }
     }
     */
    @Test
    public void getIndexIfNoAlias() throws Exception {
        Response mockResponse = Mockito.mock(Response.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        InputStream responseContent = IOUtils.toInputStream("{\"twitter\":{},\"collection\":{\"aliases\":{\"collection-1\":{},\"collection-2\":{}}}}");
        try {
            Mockito.when(mockEntity.getContent()).thenReturn(responseContent);
            Mockito.when(restClient.performRequest(new Request("GET", "/_aliases")))
              .thenReturn(mockResponse);

            ElasticSearchIndexLoader esil = new ElasticSearchIndexLoader(plugin);
            Collection<String> load = esil.load(ElasticSearchConstants.INDEXES);
            TestCase.assertEquals(3, load.size());
            TestCase.assertTrue(load.contains("twitter"));
            TestCase.assertTrue(load.contains("collection-1"));
            TestCase.assertTrue(load.contains("collection-2"));
        } finally {
            IOUtils.closeQuietly(responseContent);
        }
    }
}
