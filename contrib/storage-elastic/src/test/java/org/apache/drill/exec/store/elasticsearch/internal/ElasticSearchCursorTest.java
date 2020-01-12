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

package org.apache.drill.exec.store.elasticsearch.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchTestConstants;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class ElasticSearchCursorTest {

  @Mock
  RestClient restClient;

  private ObjectMapper objectMapper;

  @Before
  public void before() {
    this.objectMapper = new ObjectMapper();
    this.restClient = Mockito.mock(RestClient.class);
  }

  @Test
  public void testGetAllOnFirstAttempt() throws IOException {
    Response mockResponseFirstScroll = Mockito.mock(Response.class);
    HttpEntity mockEntityFirstScroll = Mockito.mock(HttpEntity.class);
    Mockito.when(mockResponseFirstScroll.getEntity()).thenReturn(mockEntityFirstScroll);
    InputStream responseFirstScroll = IOUtils.toInputStream("{\"_scroll_id\":\"DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAqFmR1eU1hbmxiUURDQjhpNmRxWWpZTXcAAAAAAAAAKBZkdXlNYW5sYlFEQ0I4aTZkcVlqWU13AAAAAAAAACkWZHV5TWFubGJRRENCOGk2ZHFZallNdw==\",\"took\":22,\"timed_out\":false,\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0},\"hits\":{\"total\":10,\"max_score\":1.0,\"hits\":[{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer02\",\"_score\":1.0,\"_source\":{\"name\":\"developer2\",\"employeeId\":3,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer05\",\"_score\":1.0,\"_source\":{\"name\":\"developer5\",\"employeeId\":5,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer08\",\"_score\":1.0,\"_source\":{\"name\":\"developer8\",\"employeeId\":8,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer12\",\"_score\":1.0,\"_source\":{\"name\":\"developer12\",\"employeeId\":12,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer16\",\"_score\":1.0,\"_source\":{\"name\":\"developer16\",\"employeeId\":17,\"department\":\"IT\",\"reportsTo\":\"manager2\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer04\",\"_score\":1.0,\"_source\":{\"name\":\"developer4\",\"employeeId\":4,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer06\",\"_score\":1.0,\"_source\":{\"name\":\"developer6\",\"employeeId\":6,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer07\",\"_score\":1.0,\"_source\":{\"name\":\"developer7\",\"employeeId\":7,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer09\",\"_score\":1.0,\"_source\":{\"name\":\"developer9\",\"employeeId\":9,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer10\",\"_score\":1.0,\"_source\":{\"name\":\"developer10\",\"employeeId\":10,\"department\":\"IT\",\"reportsTo\":\"manager1\"}}]}}");
    Mockito.when(mockEntityFirstScroll.getContent()).thenReturn(responseFirstScroll);

    Request searchRequest = new Request("POST", "/" + ElasticSearchTestConstants.EMPLOYEE_IDX + "/" + ElasticSearchTestConstants.DEVELOPER_MAPPING + "/_search");

    Mockito.when(restClient.performRequest(searchRequest)).thenReturn(mockResponseFirstScroll);

    ElasticSearchCursor esc = ElasticSearchCursor.scroll(restClient, objectMapper, "employee", "developer", MapUtils.EMPTY_MAP, null);
    TestCase.assertNotNull(esc);
    TestCase.assertNotNull(esc.hasNext());
    long counter = 0;
    while (esc.hasNext()) {
      JsonNode next = esc.next();
      TestCase.assertNotNull(next);
      counter++;
    }
    TestCase.assertEquals(10, counter);
  }


  @Test
  public void testGetAllOnTwoAttempts() throws IOException {
    Response mockResponseFirstScroll = Mockito.mock(Response.class);
    HttpEntity mockEntityFirstScroll = Mockito.mock(HttpEntity.class);
    Mockito.when(mockResponseFirstScroll.getEntity()).thenReturn(mockEntityFirstScroll);
    InputStream responseFirstScroll = IOUtils.toInputStream("{\"_scroll_id\":\"DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAqFmR1eU1hbmxiUURDQjhpNmRxWWpZTXcAAAAAAAAAKBZkdXlNYW5sYlFEQ0I4aTZkcVlqWU13AAAAAAAAACkWZHV5TWFubGJRRENCOGk2ZHFZallNdw==\",\"took\":22,\"timed_out\":false,\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0},\"hits\":{\"total\":19,\"max_score\":1.0,\"hits\":[{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer02\",\"_score\":1.0,\"_source\":{\"name\":\"developer2\",\"employeeId\":3,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer05\",\"_score\":1.0,\"_source\":{\"name\":\"developer5\",\"employeeId\":5,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer08\",\"_score\":1.0,\"_source\":{\"name\":\"developer8\",\"employeeId\":8,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer12\",\"_score\":1.0,\"_source\":{\"name\":\"developer12\",\"employeeId\":12,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer16\",\"_score\":1.0,\"_source\":{\"name\":\"developer16\",\"employeeId\":17,\"department\":\"IT\",\"reportsTo\":\"manager2\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer04\",\"_score\":1.0,\"_source\":{\"name\":\"developer4\",\"employeeId\":4,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer06\",\"_score\":1.0,\"_source\":{\"name\":\"developer6\",\"employeeId\":6,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer07\",\"_score\":1.0,\"_source\":{\"name\":\"developer7\",\"employeeId\":7,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer09\",\"_score\":1.0,\"_source\":{\"name\":\"developer9\",\"employeeId\":9,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer10\",\"_score\":1.0,\"_source\":{\"name\":\"developer10\",\"employeeId\":10,\"department\":\"IT\",\"reportsTo\":\"manager1\"}}]}}");
    Mockito.when(mockEntityFirstScroll.getContent()).thenReturn(responseFirstScroll);

    Request searchRequest = new Request("POST", "/" + ElasticSearchTestConstants.EMPLOYEE_IDX + "/" + ElasticSearchTestConstants.DEVELOPER_MAPPING + "/_search");

    Mockito.when(restClient.performRequest(searchRequest)).thenReturn(mockResponseFirstScroll);

    Response mockResponseSecondScroll = Mockito.mock(Response.class);
    HttpEntity mockEntitySecondScroll = Mockito.mock(HttpEntity.class);
    Mockito.when(mockResponseSecondScroll.getEntity()).thenReturn(mockEntitySecondScroll);
    InputStream responseSecondScroll = IOUtils.toInputStream("{\"_scroll_id\":\"DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAqFmR1eU1hbmxiUURDQjhpNmRxWWpZTXcAAAAAAAAAKBZkdXlNYW5sYlFEQ0I4aTZkcVlqWU13AAAAAAAAACkWZHV5TWFubGJRRENCOGk2ZHFZallNdw==\",\"took\":15,\"timed_out\":false,\"terminated_early\":true,\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0},\"hits\":{\"total\":19,\"max_score\":1.0,\"hits\":[{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer11\",\"_score\":1.0,\"_source\":{\"name\":\"developer11\",\"employeeId\":11,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer18\",\"_score\":1.0,\"_source\":{\"name\":\"developer18\",\"employeeId\":19,\"department\":\"IT\",\"reportsTo\":\"manager2\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer20\",\"_score\":1.0,\"_source\":{\"name\":\"developer20\",\"employeeId\":21,\"department\":\"IT\",\"reportsTo\":\"manager2\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer01\",\"_score\":1.0,\"_source\":{\"name\":\"developer1\",\"employeeId\":2,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer13\",\"_score\":1.0,\"_source\":{\"name\":\"developer13\",\"employeeId\":13,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer14\",\"_score\":1.0,\"_source\":{\"name\":\"developer14\",\"employeeId\":14,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer15\",\"_score\":1.0,\"_source\":{\"name\":\"developer15\",\"employeeId\":15,\"department\":\"IT\",\"reportsTo\":\"manager1\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer17\",\"_score\":1.0,\"_source\":{\"name\":\"developer17\",\"employeeId\":18,\"department\":\"IT\",\"reportsTo\":\"manager2\"}},{\"_index\":\"employee\",\"_type\":\"developer\",\"_id\":\"developer19\",\"_score\":1.0,\"_source\":{\"name\":\"developer19\",\"employeeId\":20,\"department\":\"IT\",\"reportsTo\":\"manager2\"}}]}}");
    Mockito.when(mockEntitySecondScroll.getContent()).thenReturn(responseSecondScroll);

    Request scrollSearchRequest = new Request("POST", "/_search/scroll");

    Mockito.when(restClient.performRequest(scrollSearchRequest)).thenReturn(mockResponseSecondScroll);

    ElasticSearchCursor esc = ElasticSearchCursor.scroll(this.restClient, this.objectMapper, "employee", "developer", MapUtils.EMPTY_MAP, null);
    TestCase.assertNotNull(esc);
    TestCase.assertNotNull(esc.hasNext());
    long counter = 0;
    while (esc.hasNext()) {
      JsonNode next = esc.next();
      TestCase.assertNotNull(next);
      counter++;
    }
    TestCase.assertEquals(19, counter);
  }

}
