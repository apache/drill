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
package org.apache.drill.exec.ssl;

import java.util.function.BiFunction;

/**
 * Provides an implementation of credentials provider.
 */
abstract class SSLCredentialsProvider {

  /**
   * Provides a concrete implementation of {@link SSLCredentialsProvider}.
   *
   * @param getPropertyMethod a reference to a method to retrieve credentials from config:
   *                          <ul>
   *                            <li>String parameter1 - property name</li>
   *                            <li>String parameter2 - default value</li>
   *                            <li>returns the property value or default value</li>
   *                          </ul>
   * @param getPasswordPropertyMethod the same as {@code getPropertyMethod} but used to
   *                          retrieve sensible data, such as keystore password,
   *                          using Hadoop's CredentialProvider API with fallback
   *                          to standard means as is used for {@code getPropertyMethod}
   * @param mode              CLIENT or SERVER
   * @return concrete implementation of SSLCredentialsProvider
   */
  static SSLCredentialsProvider getSSLCredentialsProvider(BiFunction<String, String, String> getPropertyMethod,
      BiFunction<String, String, String> getPasswordPropertyMethod, SSLConfig.Mode mode) {
    return new SSLCredentialsProviderImpl(getPropertyMethod, getPasswordPropertyMethod);
  }

  abstract String getTrustStoreType(String propertyName, String defaultValue);

  abstract String getTrustStoreLocation(String propertyName, String defaultValue);

  abstract String getTrustStorePassword(String propertyName, String defaultValue);

  abstract String getKeyStoreType(String propertyName, String defaultValue);

  abstract String getKeyStoreLocation(String propertyName, String defaultValue);

  abstract String getKeyStorePassword(String propertyName, String defaultValue);

  abstract String getKeyPassword(String propertyName, String defaultValue);


  /**
   * Default implementation of {@link SSLCredentialsProvider}.
   * Delegates retrieving credentials to a class where it is used.
   */
  private static class SSLCredentialsProviderImpl extends SSLCredentialsProvider {

    private final BiFunction<String, String, String> getPropertyMethod;
    private final BiFunction<String, String, String> getPasswordPropertyMethod;

    private SSLCredentialsProviderImpl(BiFunction<String, String, String> getPropertyMethod,
                                       BiFunction<String, String, String> getPasswordPropertyMethod) {
      this.getPropertyMethod = getPropertyMethod;
      this.getPasswordPropertyMethod = getPasswordPropertyMethod;
    }

    @Override
    String getTrustStoreType(String propertyName, String defaultValue) {
      return getPropertyMethod.apply(propertyName, defaultValue);
    }

    @Override
    String getTrustStoreLocation(String propertyName, String defaultValue) {
      return getPropertyMethod.apply(propertyName, defaultValue);
    }

    @Override
    String getTrustStorePassword(String propertyName, String defaultValue) {
      return getPasswordPropertyMethod.apply(propertyName, defaultValue);
    }

    @Override
    String getKeyStoreType(String propertyName, String defaultValue) {
      return getPropertyMethod.apply(propertyName, defaultValue);
    }

    @Override
    String getKeyStoreLocation(String propertyName, String defaultValue) {
      return getPropertyMethod.apply(propertyName, defaultValue);
    }

    @Override
    String getKeyStorePassword(String propertyName, String defaultValue) {
      return getPasswordPropertyMethod.apply(propertyName, defaultValue);
    }

    @Override
    String getKeyPassword(String propertyName, String defaultValue) {
      return getPasswordPropertyMethod.apply(propertyName, defaultValue);
    }
  }
}
