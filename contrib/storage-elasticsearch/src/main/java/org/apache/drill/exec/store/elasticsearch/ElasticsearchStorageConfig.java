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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.drill.common.logical.AbstractSecuredStoragePluginConfig;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName(ElasticsearchStorageConfig.NAME)
public class ElasticsearchStorageConfig extends AbstractSecuredStoragePluginConfig {
  public static final String NAME = "elastic";

  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writerFor(List.class);

  private final List<String> hosts;

  @JsonCreator
  public ElasticsearchStorageConfig(
      @JsonProperty("hosts") List<String> hosts,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider) {
    super(CredentialProviderUtils.getCredentialsProvider(username, password, credentialsProvider), credentialsProvider == null);
    this.hosts = hosts;
  }

  public List<String> getHosts() {
    return hosts;
  }

  public String getUsername() {
    if (directCredentials) {
      return getUsernamePasswordCredentials().getUsername();
    }
    return null;
  }

  public String getPassword() {
    if (directCredentials) {
      return getUsernamePasswordCredentials().getPassword();
    }
    return null;
  }

  @JsonIgnore
  public UsernamePasswordCredentials getUsernamePasswordCredentials() {
    return new UsernamePasswordCredentials(credentialsProvider);
  }

  @JsonIgnore
  public Map<String, Object> toConfigMap()
      throws JsonProcessingException {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("hosts", OBJECT_WRITER.writeValueAsString(hosts));

    builder.putAll(credentialsProvider.getCredentials());
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ElasticsearchStorageConfig that = (ElasticsearchStorageConfig) o;
    return Objects.equals(hosts, that.hosts)
        && Objects.equals(credentialsProvider, that.credentialsProvider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hosts, credentialsProvider);
  }
}
