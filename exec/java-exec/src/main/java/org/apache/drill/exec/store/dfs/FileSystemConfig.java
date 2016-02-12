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
package org.apache.drill.exec.store.dfs;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(FileSystemConfig.NAME)
public class FileSystemConfig extends StoragePluginConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemConfig.class);
  public static final String NAME = "file";
  public String connection;
  public Map<String, String> config;
  public Map<String, WorkspaceConfig> workspaces;
  public Map<String, FormatPluginConfig> formats;

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((config == null) ? 0 : config.hashCode());
    result = prime * result + ((connection == null) ? 0 : connection.hashCode());
    result = prime * result + ((formats == null) ? 0 : formats.hashCode());
    result = prime * result + ((workspaces == null) ? 0 : workspaces.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FileSystemConfig other = (FileSystemConfig) obj;
    if (connection == null) {
      if (other.connection != null) {
        return false;
      }
    } else if (!connection.equals(other.connection)) {
      return false;
    }
    if (formats == null) {
      if (other.formats != null) {
        return false;
      }
    } else if (!formats.equals(other.formats)) {
      return false;
    }
    if (workspaces == null) {
      if (other.workspaces != null) {
        return false;
      }
    } else if (!workspaces.equals(other.workspaces)) {
      return false;
    }
    if (config == null) {
      if (other.config != null) {
        return false;
      }
    } else if (!config.equals(other.config)) {
      return false;
    }
    return true;
  }


}
