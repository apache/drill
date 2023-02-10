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
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonTypeName(ElasticsearchStorageConfig.NAME)
public class ElasticsearchStorageConfig extends StoragePluginConfig {
  public static final String NAME = "elastic";

  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writerFor(List.class);

  private static final String HOSTS = "hosts";

  private static final String PATH_PREFIX = "pathPrefix";

  private static final String USERNAME = "username";

  private static final String PASSWORD = "password";

  public static final String CREDENTIALS_PROVIDER = "credentialsProvider";

  private static final String EMPTY_STRING = "";

  private final List<String> hosts;
  private final String pathPrefix;

  @JsonCreator
  public ElasticsearchStorageConfig(
      @JsonProperty(HOSTS) List<String> hosts,
      @JsonProperty(USERNAME) String username,
      @JsonProperty(PASSWORD) String password,
      @JsonProperty(PATH_PREFIX) String pathPrefix,
      @JsonProperty("authMode") String authMode,
      @JsonProperty(CREDENTIALS_PROVIDER) CredentialsProvider credentialsProvider) {
    super(CredentialProviderUtils.getCredentialsProvider(username, password, credentialsProvider),
        credentialsProvider == null, AuthMode.parseOrDefault(authMode, AuthMode.SHARED_USER));
    this.hosts = hosts;
    this.pathPrefix = pathPrefix;
  }

  private ElasticsearchStorageConfig(ElasticsearchStorageConfig that, CredentialsProvider credentialsProvider) {
    super(getCredentialsProvider(credentialsProvider), credentialsProvider == null, that.authMode);
    this.hosts = that.hosts;
    this.pathPrefix = that.pathPrefix;
  }

  @Override
  public ElasticsearchStorageConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new ElasticsearchStorageConfig(this, credentialsProvider);
  }

  private static CredentialsProvider getCredentialsProvider(CredentialsProvider credentialsProvider) {
    return credentialsProvider != null ? credentialsProvider : PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER;
  }

  @JsonIgnore
  public Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials() {
    return new UsernamePasswordCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  /**
   * Gets the credentials. This method is used when user translation is enabled.
   * @return An {@link Optional} containing {@link UsernamePasswordCredentials} from the config.
   */
  @JsonIgnore
  public Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials(String username) {
    return new UsernamePasswordCredentials.Builder()
        .setCredentialsProvider(credentialsProvider)
        .setQueryUser(username)
        .build();
  }

  @JsonIgnore
  public Map<String, Object> toConfigMap()
      throws JsonProcessingException {
    Map<String, String> credentials = new HashMap<>(credentialsProvider.getCredentials());
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put(HOSTS, OBJECT_WRITER.writeValueAsString(hosts));
    builder.put(PATH_PREFIX, pathPrefix != null ? pathPrefix : EMPTY_STRING);
    builder.put(USERNAME, credentials.getOrDefault(USERNAME, EMPTY_STRING));
    builder.put(PASSWORD, credentials.getOrDefault(PASSWORD, EMPTY_STRING));

    credentials.remove(USERNAME);
    credentials.remove(PASSWORD);
    builder.putAll(credentials);
    return builder.build();
  }

  /**
   * This method is used when user translation is enabled.
   * @param queryUser The user who submitted the query
   * @return A map of the configuration details.
   * @throws JsonProcessingException If JSON is unparsable, throw exception
   */
  @JsonIgnore
  public Map<String, Object> toConfigMap(String queryUser)
      throws JsonProcessingException {
    Map<String, String> credentials = new HashMap<>(credentialsProvider.getUserCredentials(queryUser));
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put(HOSTS, OBJECT_WRITER.writeValueAsString(hosts));
    builder.put(PATH_PREFIX, pathPrefix != null ? pathPrefix : EMPTY_STRING);
    builder.put(USERNAME, credentials.getOrDefault(USERNAME, EMPTY_STRING));
    builder.put(PASSWORD, credentials.getOrDefault(PASSWORD, EMPTY_STRING));

    credentials.remove(USERNAME);
    credentials.remove(PASSWORD);
    builder.putAll(credentials);
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
    return Objects.equals(hosts, that.hosts) &&
        Objects.equals(pathPrefix, that.pathPrefix) &&
        Objects.equals(credentialsProvider, that.credentialsProvider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hosts, pathPrefix, credentialsProvider);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("hosts", hosts)
        .field("pathPrefix", pathPrefix)
        .field("credentialsProvider", credentialsProvider)
        .toString();
  }
}
