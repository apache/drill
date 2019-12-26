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
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;

public class ElasticSearchGroupScanTest {

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
        Mockito.when(plugin.getConfig()).thenReturn(null);
    }

    @Test
    public void testIndexTypeMappingWithDocuments() throws IOException {
        Response mockResponseNumDocs = Mockito.mock(Response.class);
        HttpEntity mockEntityNumDocs = Mockito.mock(HttpEntity.class);
        Mockito.when(mockResponseNumDocs.getEntity()).thenReturn(mockEntityNumDocs);
        InputStream responseContentNumDocs = IOUtils.toInputStream("{\"count\":2,\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0}}");
        Mockito.when(mockEntityNumDocs.getContent()).thenReturn(responseContentNumDocs);

        Response mockResponseFirstDoc = Mockito.mock(Response.class);
        HttpEntity mockEntityFirstDoc = Mockito.mock(HttpEntity.class);
        Mockito.when(mockResponseFirstDoc.getEntity()).thenReturn(mockEntityFirstDoc);
        InputStream responseContentFirstDoc = IOUtils.toInputStream("{\"took\":28,\"timed_out\":false,\"terminated_early\":true,\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0},\"hits\":{\"total\":1,\"max_score\":1.0,\"hits\":[{\"_index\":\"employee\",\"_type\":\"manager\",\"_id\":\"manager1\",\"_score\":1.0,\"_source\":{    \"name\" : \"manager1\",    \"employeeId\" : 1,    \"department\" : \"IT\"}}]}}");
        Mockito.when(mockEntityFirstDoc.getContent()).thenReturn(responseContentFirstDoc);
        try {
            Mockito.when(restClient
              .performRequest("GET", "/"+ElasticSearchTestConstants.EMPLOYEE_IDX+"/"+ElasticSearchTestConstants.MANAGER_MAPPING+"/_count"))
              .thenReturn(mockResponseNumDocs);
            Mockito.when(restClient
              .performRequest("GET", "/"+ElasticSearchTestConstants.EMPLOYEE_IDX+"/"+ElasticSearchTestConstants.MANAGER_MAPPING+"/_search?size=1&terminate_after=1"))
              .thenReturn(mockResponseFirstDoc);

            ElasticSearchGroupScan esgp = new ElasticSearchGroupScan("testuser",
                    plugin,
                    new ElasticSearchScanSpec(ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.MANAGER_MAPPING),
                    null);
            ScanStats scanStats = esgp.getScanStats();
            TestCase.assertNotNull(scanStats);
            TestCase.assertEquals(1f,scanStats.getCpuCost());
            TestCase.assertEquals(2,scanStats.getRecordCount());
            TestCase.assertEquals(264f,scanStats.getDiskCost());
            TestCase.assertEquals(ScanStats.GroupScanProperty.EXACT_ROW_COUNT,scanStats.getGroupScanProperty());
        } finally {
            IOUtils.closeQuietly(responseContentNumDocs);
            IOUtils.closeQuietly(responseContentFirstDoc);
        }
    }

    @Test
    public void testIndexTypeMappingWithNoDocuments() throws IOException {
        Response mockResponseNumDocs = Mockito.mock(Response.class);
        HttpEntity mockEntityNumDocs = Mockito.mock(HttpEntity.class);
        Mockito.when(mockResponseNumDocs.getEntity()).thenReturn(mockEntityNumDocs);
        InputStream responseContentNumDocs = IOUtils.toInputStream("{\"count\":0,\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0}}");
        Mockito.when(mockEntityNumDocs.getContent()).thenReturn(responseContentNumDocs);

        try {
            Mockito.when(this.restClient.performRequest("GET", "/"+ElasticSearchTestConstants.EMPLOYEE_IDX+"/"+ElasticSearchTestConstants.MANAGER_MAPPING+"/_count")).thenReturn(mockResponseNumDocs);

            ElasticSearchGroupScan esgp = new ElasticSearchGroupScan("testuser",
                    plugin,
                    new ElasticSearchScanSpec(ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.MANAGER_MAPPING),
                    null);
            ScanStats scanStats = esgp.getScanStats();
            TestCase.assertNotNull(scanStats);
            TestCase.assertEquals(1f,scanStats.getCpuCost());
            TestCase.assertEquals(0,scanStats.getRecordCount());
            TestCase.assertEquals(0f,scanStats.getDiskCost());
            TestCase.assertEquals(ScanStats.GroupScanProperty.EXACT_ROW_COUNT,scanStats.getGroupScanProperty());
        } finally {
            IOUtils.closeQuietly(responseContentNumDocs);
        }
    }
}
