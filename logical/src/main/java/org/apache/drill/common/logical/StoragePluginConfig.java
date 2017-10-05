/**
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


import com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
public abstract class StoragePluginConfig{

  private boolean enabled = true;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

  /**
   * Function to enable the security setting for storage plugin and return true/false
   * based on any modification was done in config or not.
   * @return - true - if there was any modification
   *         - false - if no modification was done
   */
  public boolean enableSecurity() {
    // no-op by default
    return false;
  }

  /**
   * Function to disable the security setting for storage plugin and return true/false
   * based on any modification was done in config or not.
   *
   * @return - true - if there was any modification
   *         - false - if no modification was done
   */
  public boolean disableSecurity() {
    // no-op by default
    return false;
  }

  /**
   * Verify if security is enabled/disabled in StoragePluginConfig as compared to
   * Drill config to enable/disable security of StoragePlugin
   * @param enablePluginSecurity - flag to indicate if Drill is configured to enable security
   *                             for StoragePlugins.
   * @return true -  Plugin security config matches enablePluginSecurity. i.e. plugin security is enabled
   *                 when enablePluginSecurity is true and disabled when enablePluginSecurity is false
   *         false - Plugin security config doesn't matches enablePluginSecurity. Opposite of above case.
   */
  public boolean isValidSecurityConfig(boolean enablePluginSecurity) {
    return true;
  }

}
