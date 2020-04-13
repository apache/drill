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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class HttpAPIConfig {
  private static final Logger logger = LoggerFactory.getLogger(HttpAPIConfig.class);

  private final String url;

  private final HttpMethods method;

  private final Map<String, String> headers;

  private final String authType;

  private final String userName;

  private final String password;

  private final String postBody;

  public enum HttpMethods {
    /**
     * Value for HTTP GET method
     */
    GET,
    /**
     * Value for HTTP POST method
     */
    POST;
  }

  public HttpAPIConfig(@JsonProperty("url") String url,
                       @JsonProperty("method") String method,
                       @JsonProperty("headers") Map<String, String> headers,
                       @JsonProperty("authType") String authType,
                       @JsonProperty("userName") String userName,
                       @JsonProperty("password") String password,
                       @JsonProperty("postBody") String postBody) {

    this.headers = headers;
    this.method = Strings.isNullOrEmpty(method)
        ? HttpMethods.GET : HttpMethods.valueOf(method.trim().toUpperCase());

    // Get the request method.  Only accept GET and POST requests.  Anything else will default to GET.
    switch (this.method) {
      case GET:
      case POST:
        break;
      default:
        throw UserException
          .validationError()
          .message("Invalid HTTP method: %s.  Drill supports 'GET' and , 'POST'.", method)
          .build(logger);
    }
    if (Strings.isNullOrEmpty(url)) {
      throw UserException
        .validationError()
        .message("URL is required for the HTTP storage plugin.")
        .build(logger);
    }

    // Put a trailing slash on the URL if it is missing
    if (url.charAt(url.length() - 1) != '/') {
      this.url = url + "/";
    } else {
      this.url = url;
    }

    // Get the authentication method. Future functionality will include OAUTH2 authentication but for now
    // Accept either basic or none.  The default is none.
    this.authType = Strings.isNullOrEmpty(authType) ? "none" : authType;
    this.userName = userName;
    this.password = password;
    this.postBody = postBody;
  }

  @JsonProperty("url")
  public String url() { return url; }

  @JsonProperty("method")
  public String method() { return method.toString(); }

  @JsonProperty("headers")
  public Map<String, String> headers() { return headers; }

  @JsonProperty("authType")
  public String authType() { return authType; }

  @JsonProperty("userName")
  public String userName() { return userName; }

  @JsonProperty("password")
  public String password() { return password; }

  @JsonProperty("postBody")
  public String postBody() { return postBody; }

  @JsonIgnore
  public HttpMethods getMethodType() {
    return HttpMethods.valueOf(this.method());
  }

  @Override
  public int hashCode() {
    return Objects.hash(url, method, headers, authType, userName, password, postBody);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("url", url)
      .field("method", method)
      .field("headers", headers)
      .field("authType", authType)
      .field("username", userName)
      .field("password", password)
      .field("postBody", postBody)
      .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HttpAPIConfig other = (HttpAPIConfig) obj;
    return Objects.equals(url, other.url)
      && Objects.equals(method, other.method)
      && Objects.equals(headers, other.headers)
      && Objects.equals(authType, other.authType)
      && Objects.equals(userName, other.userName)
      && Objects.equals(password, other.password)
      && Objects.equals(postBody, other.postBody);
  }
}
