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
package org.apache.drill.common.logical;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.drill.shaded.guava.com.google.common.base.Strings;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public abstract class StoragePluginConfig {

  // DO NOT include enabled status in equality and hash
  // comparisons; doing so will break the plugin registry.
  protected Boolean enabled;

  // The overridable default plugin auth mode is DRILL_PROCESS
  protected AuthMode authMode = AuthMode.DRILL_PROCESS;

  /**
   * Check for enabled status of the plugin
   *
   * @return true, when enabled. False, when disabled or status is absent
   */
  public boolean isEnabled() {
    return enabled != null && enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public AuthMode getAuthMode() {
    return authMode;
  }

  public void setAuthMode(AuthMode authMode) {
    this.authMode = authMode;
  }

  /**
   * Allows to check whether the enabled status is present in config
   *
   * @return true if enabled status is present, false otherwise
   */
  @JsonIgnore
  public boolean isEnabledStatusPresent() {
    return enabled != null;
  }

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

  public String getValue(String key) {
    return null;
  }

  /**
   * The standardised authentication modes that storage plugins may offer.
   */
  public enum AuthMode {
    /**
     * Default. Connects using the identity of the Drill cluster (OS user or
     * service principal) if the external storage is aware of said identity,
     * otherwise connects without authentication. Unaffected by the Drill
     * query user's identity.
     */
    DRILL_PROCESS,
    /**
     * Connects using a single set of shared credentials stored in some
     * credential provider.  Unaffected by the Drill query user's identity.
     */
    SHARED_USER,
    /**
     * Depending on the plugin, connects using one of the two modes above then
     * instructs the external storage to set the identity on the connection
     * to that of the Drill query user.  User identity in the external system
     * will match the Drill query user's identity.
     */
    USER_IMPERSONATION,
    /**
     * Connects with stored credentials looked up for (translated from)
     * the Drill query user.  User identity in the external system will be
     * a function of the Drill query user's identity (1-1 or *-1) .
     */
    USER_TRANSLATION;

    public static AuthMode parseOrDefault(String authMode, AuthMode defavlt) {
      return !Strings.isNullOrEmpty(authMode) ? AuthMode.valueOf(authMode.toUpperCase()) : defavlt;
    }
  }
}
