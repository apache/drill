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
package org.apache.drill.exec.server.rest.ai;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;

/**
 * OkHttp interceptor that adds custom headers to all requests.
 * Used for enterprise authentication headers, tenant IDs, etc.
 */
public class CustomHeadersInterceptor implements Interceptor {
  private final Map<String, String> headers;

  public CustomHeadersInterceptor(Map<String, String> headers) {
    this.headers = headers;
  }

  @Override
  public Response intercept(Chain chain) throws IOException {
    Request.Builder builder = chain.request().newBuilder();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      builder.addHeader(entry.getKey(), entry.getValue());
    }
    return chain.proceed(builder.build());
  }
}
