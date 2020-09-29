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
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HttpApiConfig {
  private static final Logger logger = LoggerFactory.getLogger(HttpApiConfig.class);

  private final String url;

  /**
   * Whether this API configuration represents a schema (with the
   * table providing additional parts of the URL), or if this
   * API represents a table (the URL is complete except for
   * parameters specified in the WHERE clause.)
   */
  private final boolean requireTail;

  private final HttpMethod method;

  private final String postBody;

  private final Map<String, String> headers;

  /**
   * List of query parameters which can be used in the SQL WHERE clause
   * to push filters to the REST request as HTTP query parameters.
   */
  private final List<String> params;

  /**
   * Path within the message to the JSON object, or array of JSON
   * objects, which contain the actual data. Allows a request to
   * skip over "overhead" such as status codes. Must be a slash-delimited
   * set of JSON field names.
   */
  private final String dataPath;

  private final String authType;
  private final String userName;
  private final String password;
  private final String inputType;


  public enum HttpMethod {
    /**
     * Value for HTTP GET method
     */
    GET,
    /**
     * Value for HTTP POST method
     */
    POST;
  }

  public HttpApiConfig(@JsonProperty("url") String url,
                       @JsonProperty("method") String method,
                       @JsonProperty("headers") Map<String, String> headers,
                       @JsonProperty("authType") String authType,
                       @JsonProperty("userName") String userName,
                       @JsonProperty("password") String password,
                       @JsonProperty("postBody") String postBody,
                       @JsonProperty("params") List<String> params,
                       @JsonProperty("dataPath") String dataPath,
                       @JsonProperty("requireTail") Boolean requireTail,
                       @JsonProperty("inputType") String inputType) {

    this.headers = headers;
    this.method = Strings.isNullOrEmpty(method)
        ? HttpMethod.GET : HttpMethod.valueOf(method.trim().toUpperCase());
    this.url = url;

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

    // Get the authentication method. Future functionality will include OAUTH2 authentication but for now
    // Accept either basic or none.  The default is none.
    this.authType = Strings.isNullOrEmpty(authType) ? "none" : authType;
    this.userName = userName;
    this.password = password;
    this.postBody = postBody;
    this.params = params == null || params.isEmpty() ? null :
      ImmutableList.copyOf(params);
    this.dataPath = Strings.isNullOrEmpty(dataPath) ? null : dataPath;

    // Default to true for backward compatibility with first PR.
    this.requireTail = requireTail == null ? true : requireTail;

    this.inputType = inputType == null
      ? "json" : inputType.trim().toLowerCase();
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

  @JsonProperty("params")
  public List<String> params() { return params; }

  @JsonProperty("dataPath")
  public String dataPath() { return dataPath; }

  @JsonProperty("requireTail")
  public boolean requireTail() { return requireTail; }

  @JsonIgnore
  public HttpMethod getMethodType() {
    return HttpMethod.valueOf(this.method());
  }

  @JsonProperty("inputType")
  public String inputType() { return inputType; }

  @Override
  public int hashCode() {
    return Objects.hash(url, method, requireTail, params, headers,
        authType, userName, password, postBody, inputType);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("url", url)
      .field("require tail", requireTail)
      .field("method", method)
      .field("dataPath", dataPath)
      .field("headers", headers)
      .field("authType", authType)
      .field("username", userName)
      .maskedField("password", password)
      .field("postBody", postBody)
      .field("filterFields", params)
      .field("inputType", inputType)
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
    HttpApiConfig other = (HttpApiConfig) obj;
    return Objects.equals(url, other.url)
      && Objects.equals(method, other.method)
      && Objects.equals(headers, other.headers)
      && Objects.equals(authType, other.authType)
      && Objects.equals(userName, other.userName)
      && Objects.equals(password, other.password)
      && Objects.equals(postBody, other.postBody)
      && Objects.equals(params, other.params)
      && Objects.equals(dataPath, other.dataPath)
      && Objects.equals(requireTail, other.requireTail)
      && Objects.equals(inputType, other.inputType);
  }
}
