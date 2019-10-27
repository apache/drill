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

import org.apache.drill.exec.store.druid.druid.DruidSelectResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.HttpEntity;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.assertj.core.api.Assertions.assertThat;

public class DruidQueryClientTest {

  @Mock
  private RestClient restClient;

  @Mock
  private HttpResponse httpResponse;

  @Mock
  private StatusLine statusLine;

  @Mock
  private HttpEntity httpEntity;

  private DruidQueryClient druidQueryClient;
  private static final String BROKER_URI = "some broker uri";
  private static final String QUERY = "some query";
  private static final Header ENCODING_HEADER =
      new BasicHeader(HttpHeaders.CONTENT_ENCODING, StandardCharsets.UTF_8.name());

  @Before
  public void setup() throws IOException {
    restClient = mock(RestClient.class);
    httpResponse = mock(HttpResponse.class);
    statusLine = mock(StatusLine.class);
    httpEntity = mock(HttpEntity.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpEntity.getContentEncoding()).thenReturn(ENCODING_HEADER);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(restClient.post(BROKER_URI + "/druid/v2", QUERY))
        .thenReturn(httpResponse);

    druidQueryClient = new DruidQueryClient(BROKER_URI, restClient);
  }

  @Test(expected=Exception.class)
  public void executeQueryCalledDruidReturnsNon200ShouldThrowError()
      throws Exception {
    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    druidQueryClient.executeQuery(QUERY);
  }

  @Test
  public void executeQueryCalledNoResponsesFoundReturnsEmptyEventList()
      throws Exception {
    InputStream inputStream =
        new ByteArrayInputStream("[]".getBytes(StandardCharsets.UTF_8.name()));
    when(httpEntity.getContent()).thenReturn(inputStream);

    DruidSelectResponse response = druidQueryClient.executeQuery(QUERY);
    assertThat(response.getEvents()).isEmpty();
    assertThat(response.getPagingIdentifiers()).isEmpty();
  }

  @Test
  public void executeQueryCalledSuccessfullyParseQueryResults()
      throws Exception {
    String result = "[{\"result\":{\"pagingIdentifiers\":{\"some_segment_identifier\":500,\"some_other_segment_identifier\":501},\"events\":[{\"event\":{\"some_property\":\"some value\"}},{\"event\":{\"some_property\":\"some other value\"}}]}}]";
    InputStream inputStream =
        new ByteArrayInputStream(result.getBytes(StandardCharsets.UTF_8.name()));
    when(httpEntity.getContent()).thenReturn(inputStream);

    DruidSelectResponse response = druidQueryClient.executeQuery(QUERY);
    assertThat(response.getEvents()).isNotEmpty();
    assertThat(response.getEvents().size()).isEqualTo(2);
    assertThat(response.getEvents().get(0).get("some_property").textValue()).isEqualTo("some value");
    assertThat(response.getEvents().get(1).get("some_property").textValue()).isEqualTo("some other value");
  }
}
