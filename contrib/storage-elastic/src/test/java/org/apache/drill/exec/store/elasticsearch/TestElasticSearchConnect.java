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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.MapUtils;
import org.apache.drill.exec.store.elasticsearch.internal.ElasticSearchCursor;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Ignore("requires remote ElasticSearch server")
public class TestElasticSearchConnect {

  private static final Logger logger = LoggerFactory.getLogger(TestElasticSearchConnect.class);

  private static final String HOST = "127.0.0.1";

  private static final String CREDENTIALS = "elastic:changeme";

  private static final int PORT = 9200;

  private static final int MAXRETRYTIMEOUTMILLIS = 1000;


  private RestClient createClient(Header[] headers) {
    RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(HOST, PORT, "http"));
    if (headers != null && headers.length > 0) {
      clientBuilder.setDefaultHeaders(headers);
    }
    clientBuilder.setMaxRetryTimeoutMillis(MAXRETRYTIMEOUTMILLIS);
    return clientBuilder.build();
  }

  @Test
  public void TryConnectWithoutCredentials() throws IOException {
    RestClient restClient = createClient(null);
    try {
      Response response = restClient.performRequest("GET", "/_nodes");
      TestCase.assertNotNull(response);
    } catch (Exception e) {
      assert (e.getMessage().contains("HTTP/1.1 401 Unauthorized"));
    } finally {
      restClient.close();
    }
  }

  @Test
  public void TryConnectWithCredentials() throws IOException {

    List<BasicHeader> headers = Arrays.asList(new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(CREDENTIALS.getBytes())));
    RestClient restClient = this.createClient((Header[]) headers.toArray());
    try {
      Response response = restClient.performRequest("GET", "/_nodes");
      TestCase.assertNotNull(response);
    } finally {
      restClient.close();
    }
  }

  @Test
  public void testRealCursoring() throws IOException {

    List<BasicHeader> headers = Arrays.asList(new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(CREDENTIALS.getBytes())));

    ElasticSearchCursor esc = ElasticSearchCursor.scroll(createClient((Header[]) headers.toArray()), new ObjectMapper(), "employee", "developer", MapUtils.EMPTY_MAP, null);
    TestCase.assertTrue(esc.hasNext());
    int count = 0;
    while (esc.hasNext()) {
      JsonNode next = esc.next();
      TestCase.assertNotNull(next);
      logger.info(next.toString());
      count++;
    }
    TestCase.assertEquals(19, count);
  }
}
