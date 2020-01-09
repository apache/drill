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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ElasticSearchPluginConfig.NAME)
public class ElasticSearchPluginConfig extends StoragePluginConfigBase {
  public static final String NAME = "elasticsearch";

  private static final int DEFAULT_PORT = 9200;

  private static final long DEFAULT_CACHE_DURATION = 5;

  private static final TimeUnit DEFAULT_CACHE_TIMEUNIT = TimeUnit.MINUTES;

  public final String hostsAndPorts;

  public final String credentials;

  public final String pathPrefix;

  private final Integer hashCode;

  public final int maxRetryTimeoutMillis;

  public final long cacheDuration;

  public final TimeUnit cacheTimeUnit;


  /**
   * Create ElasticSearch Plugin configuration
   *
   * @param credentials   its format should be "[USERNAME]:[PASSWORD]". For example: 'me:myPassword'. In case of null or empty String, no Authorization will be used
   * @param hostsAndPorts its format is a list of a [PROTOCOL]://[HOST]:[PORT] separated by ','. For example: 'http://localhost:9200,http://localhost:9201'
   */
  public ElasticSearchPluginConfig(@JsonProperty(value = "credentials") String credentials,
                                   @JsonProperty(value = "hostsAndPorts", required = true) String hostsAndPorts,
                                   @JsonProperty(value = "pathPrefix") String pathPrefix,
                                   @JsonProperty(value = "maxRetryTimeoutMillis") int maxRetryTimeoutMillis,
                                   @JsonProperty(value = "cacheDuration") long cacheDuration,
                                   @JsonProperty(value = "cacheTimeUnit") TimeUnit cacheTimeUnit) {
    if (!StringUtils.isEmpty(credentials)) {
      // Account password
      this.credentials = credentials;
    } else {
      this.credentials = null;
    }
    this.hostsAndPorts = hostsAndPorts;
    this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
    this.pathPrefix = pathPrefix;

    if (cacheDuration > 0) {
      this.cacheDuration = cacheDuration;
    } else {
      this.cacheDuration = DEFAULT_CACHE_DURATION;
    }

    if (cacheTimeUnit != null) {
      this.cacheTimeUnit = cacheTimeUnit;
    } else {
      this.cacheTimeUnit = DEFAULT_CACHE_TIMEUNIT;
    }


    // TODO Fix this...
    // Building hashcode
    HashCodeBuilder builder = new HashCodeBuilder(13, 7);
    builder
      .append(this.hostsAndPorts)
      .append(this.credentials)
      .append(this.maxRetryTimeoutMillis)
      .append(this.pathPrefix)
      .append(this.cacheDuration)
      .append(this.cacheTimeUnit);
    hashCode = builder.build();
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    ElasticSearchPluginConfig thatConfig = (ElasticSearchPluginConfig) that;
    return hostsAndPorts.equals(thatConfig.hostsAndPorts) &&
      (Objects.equals(credentials, thatConfig.credentials)) &&
      maxRetryTimeoutMillis == thatConfig.maxRetryTimeoutMillis &&
      cacheDuration == thatConfig.cacheDuration &&
      (Objects.equals(pathPrefix, thatConfig.pathPrefix)) &&
      (Objects.equals(cacheTimeUnit, thatConfig.cacheTimeUnit));
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  public String getHostsAndPorts() {
    return this.hostsAndPorts;
  }

  public String getCredentials() {
    return credentials;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }

  public int getMaxRetryTimeoutMillis() {
    return maxRetryTimeoutMillis;
  }

  /**
   * Creates and returns a client base on configuration
   *
   * @return a {@link RestClient} to work against elasticSearch
   */
  @JsonIgnore
  public RestClient createClient() {
    // Client created
    RestClientBuilder clientBuilder = RestClient.builder(parseHostsAndPorts());

    Header[] headers = buildHeaders();
    if (headers != null) {
      clientBuilder.setDefaultHeaders(headers);
    }
    if (maxRetryTimeoutMillis > 0) {
      clientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);
    }
    if (!StringUtils.isEmpty(pathPrefix)) {
      // Request path prefix
      clientBuilder.setPathPrefix(pathPrefix);
    }
    return clientBuilder.build();
  }

  private Header[] buildHeaders() {
    List<Header> headers = new ArrayList<>();
    if (!StringUtils.isEmpty(this.credentials)) {
      headers.add(new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(this.credentials.getBytes())));
    }
    // account password
    return (headers.isEmpty() ? null :  headers.toArray(new Header[headers.size()]));
  }

  private HttpHost[] parseHostsAndPorts() {
    Collection<HttpHost> rtnValue = new ArrayList<>();
    String[] hostPortList = hostsAndPorts.split(",");
    for (String hostPort : hostPortList) {
      String[] split = hostPort.split(":");
      String protocol = split[0];
      String host = split[1].replaceAll("/", "");
      int port = DEFAULT_PORT;
      if (split.length > 2) {
        port = Integer.parseInt(split[2]);
      }
      // Host node
      rtnValue.add(new HttpHost(host, port, protocol));
    }
    return rtnValue.toArray(new HttpHost[0]);
  }

  public long getCacheDuration() {
    return cacheDuration;
  }

  public TimeUnit getCacheTimeUnit() {
    return cacheTimeUnit;
  }
}
