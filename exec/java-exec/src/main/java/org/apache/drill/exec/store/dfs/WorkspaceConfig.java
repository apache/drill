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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stores the workspace related config. A workspace has:
 *  - location which is a path.
 *  - writable flag to indicate whether the location supports creating new tables.
 *  - default storage format for new tables created in this workspace.
 */
public class WorkspaceConfig {

  /** Default workspace is a root directory which supports read, but not write. */
  public static final WorkspaceConfig DEFAULT = new WorkspaceConfig("/", false, null);

  private final String location;
  private final boolean writable;
  private final String storageformat;

  public WorkspaceConfig(@JsonProperty("location") String location,
                         @JsonProperty("writable") boolean writable,
                         @JsonProperty("storageformat") String storageformat) {
    this.location = location;
    this.writable = writable;
    this.storageformat = storageformat;
  }

  public String getLocation() {
    return location;
  }

  public boolean isWritable() {
    return writable;
  }

  @JsonProperty("storageformat")
  public String getStorageFormat() {
    return storageformat;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj == null || !(obj instanceof WorkspaceConfig)) {
      return false;
    }

    WorkspaceConfig that = (WorkspaceConfig) obj;
    return ((this.location == null && that.location == null) || this.location.equals(that.location)) &&
        this.writable == that.writable &&
        ((this.storageformat == null && that.storageformat == null) || this.storageformat.equals(that.storageformat));
  }
}
