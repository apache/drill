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
package org.apache.drill.exec.store.accumulo;

import com.google.common.base.Strings;

/**
 * Authentication types supported by the Accumulo storage plugin.
 */
public enum AccumuloAuthType {
  /**
   * Username/password authentication (default).
   * Uses Accumulo's PasswordToken for authentication.
   */
  PASSWORD,

  /**
   * Kerberos authentication using SASL.
   * Uses Accumulo's KerberosToken for authentication.
   * Requires a principal and keytab path to be configured.
   */
  KERBEROS;

  /**
   * Parses the authentication type from a string, with a default fallback.
   *
   * @param authType the string representation of the auth type
   * @param defaultType the default type to use if authType is null or empty
   * @return the parsed AccumuloAuthType
   */
  public static AccumuloAuthType parseOrDefault(String authType, AccumuloAuthType defaultType) {
    if (Strings.isNullOrEmpty(authType)) {
      return defaultType;
    }
    return AccumuloAuthType.valueOf(authType.toUpperCase());
  }
}
