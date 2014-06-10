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

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(FileSystemConfig.NAME)
public class FileSystemConfig extends StoragePluginConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemConfig.class);
  public static final String NAME = "file";
  public String connection;
  public Map<String, WorkspaceConfig> workspaces;
  public Map<String, FormatPluginConfig> formats;

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FileSystemConfig)) {
      return false;
    }
    FileSystemConfig that = (FileSystemConfig) obj;
    boolean same = ((this.connection == null && that.connection == null) || this.connection.equals(that.connection)) &&
            ((this.workspaces == null && that.workspaces == null) || this.workspaces.equals(that.workspaces)) &&
            ((this.formats== null && that.formats == null) || this.formats.equals(that.formats));
    return same;
  }

}
