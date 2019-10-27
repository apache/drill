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

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.http.protocol.HTTP.CONTENT_TYPE;

public class RestClientWrapper implements RestClient {
  private static final HttpClient httpClient = new DefaultHttpClient();
  private static final String DEFAULT_ENCODING = "UTF-8";

  public HttpResponse get(String url) throws IOException {
    HttpGet httpget = new HttpGet(url);
    httpget.addHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
    return httpClient.execute(httpget);
  }

  public HttpResponse post(String url, String body) throws IOException {
    HttpPost httppost = new HttpPost(url);
    httppost.addHeader(CONTENT_TYPE, APPLICATION_JSON);
    HttpEntity entity = new ByteArrayEntity(body.getBytes(DEFAULT_ENCODING));
    httppost.setEntity(entity);

    return httpClient.execute(httppost);
  }
}
