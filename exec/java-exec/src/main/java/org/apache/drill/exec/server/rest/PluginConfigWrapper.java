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

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@XmlRootElement
public class PluginConfigWrapper {

  private final String name;
  private final StoragePluginConfig config;

  @JsonCreator
  public PluginConfigWrapper(@JsonProperty("name") String name,
      @JsonProperty("config") StoragePluginConfig config) {
    this.name = name;
    this.config = config;
  }

  public String getName() { return name; }

  public StoragePluginConfig getConfig() { return config; }

  public boolean enabled() {
    return config.isEnabled();
  }

  public void createOrUpdateInStorage(StoragePluginRegistry storage) throws PluginException {
    storage.validatedPut(name, config);
  }
}
