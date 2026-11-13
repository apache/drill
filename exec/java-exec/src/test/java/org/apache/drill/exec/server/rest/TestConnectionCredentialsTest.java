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
package org.apache.drill.exec.server.rest;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.apache.drill.test.BaseTest;

/**
 * The credentials the test-connection endpoint connects with. A storage config holds them in
 * one of two shapes, and only one of them is reachable through getCredentialsProvider().
 */
public class TestConnectionCredentialsTest extends BaseTest {

  /**
   * A config in the shape plugins take when the credentials are given directly: the base
   * class reports no credentials provider, and the values are on the config itself.
   */
  private static class DirectConfig extends StoragePluginConfig {
    private final String username;
    private final String password;

    DirectConfig(String username, String password) {
      super(new PlainCredentialsProvider(new HashMap<>()), true, AuthMode.SHARED_USER);
      this.username = username;
      this.password = password;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }
  }

  /** A config whose credentials live in a provider, as the plugin forms write them. */
  private static class ProviderConfig extends StoragePluginConfig {
    ProviderConfig(CredentialsProvider provider) {
      super(provider, false, AuthMode.SHARED_USER);
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }
  }

  private static PlainCredentialsProvider provider(String username, String password) {
    Map<String, String> credentials = new HashMap<>();
    credentials.put(UsernamePasswordCredentials.USERNAME, username);
    credentials.put(UsernamePasswordCredentials.PASSWORD, password);
    return new PlainCredentialsProvider(credentials);
  }

  @Test
  public void readsCredentialsStoredInAProvider() {
    UsernamePasswordCredentials creds =
      TestConnectionResources.credentialsOf(new ProviderConfig(provider("store", "secret")));
    assertEquals("store", creds.getUsername());
    assertEquals("secret", creds.getPassword());
  }

  @Test
  public void readsCredentialsGivenDirectlyOnTheConfig() {
    // Without the fallback these configs test as an anonymous user, because a config with
    // direct credentials reports no credentials provider at all.
    UsernamePasswordCredentials creds =
      TestConnectionResources.credentialsOf(new DirectConfig("store", "secret"));
    assertEquals("store", creds.getUsername());
    assertEquals("secret", creds.getPassword());
  }

  @Test
  public void reportsNoCredentialsWhenTheConfigCarriesNone() {
    assertNull(TestConnectionResources.credentialsOf(new DirectConfig(null, null)));
  }
}
