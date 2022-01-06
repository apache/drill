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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

@Slf4j
@Builder
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpApiConfig.HttpApiConfigBuilder.class)
public class HttpApiConfig {

  protected static final String DEFAULT_INPUT_FORMAT = "json";
  protected static final String CSV_INPUT_FORMAT = "csv";
  protected static final String XML_INPUT_FORMAT = "xml";

  @JsonProperty
  private final String url;
  /**
   * Whether this API configuration represents a schema (with the
   * table providing additional parts of the URL), or if this
   * API represents a table (the URL is complete except for
   * parameters specified in the WHERE clause.)
   */
  @JsonInclude
  @JsonProperty
  private final boolean requireTail;

  @JsonProperty
  private final String method;

  @JsonProperty
  private final String postBody;

  @JsonProperty
  private final Map<String, String> headers;

  /**
   * List of query parameters which can be used in the SQL WHERE clause
   * to push filters to the REST request as HTTP query parameters.
   */
  @JsonProperty
  private final List<String> params;

  /**
   * Path within the message to the JSON object, or array of JSON
   * objects, which contain the actual data. Allows a request to
   * skip over "overhead" such as status codes. Must be a slash-delimited
   * set of JSON field names.
   */
  @JsonProperty
  private final String dataPath;

  @JsonProperty
  private final String authType;
  @JsonProperty
  private final String inputType;
  @JsonProperty
  private final int xmlDataLevel;
  @JsonProperty
  private final String limitQueryParam;
  @JsonProperty
  private final boolean errorOn400;

  // Enables the user to configure JSON options at the connection level rather than globally.
  @JsonProperty
  private final HttpJsonOptions jsonOptions;

  @JsonInclude
  @JsonProperty
  private final boolean verifySSLCert;
  @Getter(AccessLevel.NONE)
  private final CredentialsProvider credentialsProvider;
  @JsonProperty
  private final HttpPaginatorConfig paginator;

  @Getter(AccessLevel.NONE)
  protected boolean directCredentials;

  public enum HttpMethod {
    /**
     * Value for HTTP GET method
     */
    GET,
    /**
     * Value for HTTP POST method
     */
    POST
  }

  private HttpApiConfig(HttpApiConfig.HttpApiConfigBuilder builder) {
    this.headers = builder.headers;
    this.method = StringUtils.isEmpty(builder.method)
        ? HttpMethod.GET.toString() : builder.method.trim().toUpperCase();
    this.url = builder.url;
    this.jsonOptions = builder.jsonOptions;

    HttpMethod httpMethod = HttpMethod.valueOf(this.method);
    // Get the request method.  Only accept GET and POST requests.  Anything else will default to GET.
    switch (httpMethod) {
      case GET:
      case POST:
        break;
      default:
        throw UserException
          .validationError()
          .message("Invalid HTTP method: %s.  Drill supports 'GET' and , 'POST'.", method)
          .build(logger);
    }
    if (StringUtils.isEmpty(url)) {
      throw UserException
        .validationError()
        .message("URL is required for the HTTP storage plugin.")
        .build(logger);
    }

    // Get the authentication method. Future functionality will include OAUTH2 authentication but for now
    // Accept either basic or none.  The default is none.
    this.authType = StringUtils.defaultIfEmpty(builder.authType, "none");
    this.postBody = builder.postBody;
    this.params = CollectionUtils.isEmpty(builder.params) ? null :
      ImmutableList.copyOf(builder.params);
    this.dataPath = StringUtils.defaultIfEmpty(builder.dataPath, null);

    // Default to true for backward compatibility with first PR.
    this.requireTail = builder.requireTail;

    // Default to true for backward compatibility, and better security practices
    this.verifySSLCert = builder.verifySSLCert;

    this.inputType = builder.inputType.trim().toLowerCase();

    this.xmlDataLevel = Math.max(1, builder.xmlDataLevel);
    this.errorOn400 = builder.errorOn400;
    this.credentialsProvider = CredentialProviderUtils.getCredentialsProvider(builder.userName, builder.password, builder.credentialsProvider);
    this.directCredentials = builder.credentialsProvider == null;

    this.limitQueryParam = builder.limitQueryParam;
    this.paginator = builder.paginator;
  }

  @JsonProperty
  public String userName() {
    if (directCredentials) {
      return getUsernamePasswordCredentials().getUsername();
    }
    return null;
  }

  @JsonProperty
  public String password() {
    if (directCredentials) {
      return getUsernamePasswordCredentials().getPassword();
    }
    return null;
  }

  @JsonIgnore
  public HttpUrl getHttpUrl() {
    return HttpUrl.parse(this.url);
  }

  @JsonIgnore
  public HttpMethod getMethodType() {
    return HttpMethod.valueOf(this.method);
  }

  @JsonIgnore
  public UsernamePasswordCredentials getUsernamePasswordCredentials() {
    return new UsernamePasswordCredentials(credentialsProvider);
  }

  @JsonProperty
  public CredentialsProvider credentialsProvider() {
    if (directCredentials) {
      return null;
    }
    return credentialsProvider;
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpApiConfigBuilder {
    @Getter
    @Setter
    private String userName;

    @Getter
    @Setter
    private String password;

    @Getter
    @Setter
    private boolean requireTail = true;

    @Getter
    @Setter
    private boolean verifySSLCert = true;

    @Getter
    @Setter
    private String inputType = DEFAULT_INPUT_FORMAT;

    public HttpApiConfig build() {
      return new HttpApiConfig(this);
    }
  }
}
