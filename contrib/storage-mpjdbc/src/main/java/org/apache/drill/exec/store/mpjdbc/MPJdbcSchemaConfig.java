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

package org.apache.drill.exec.store.mpjdbc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stores the workspace related config. A workspace has: - location which is a
 * path. - writable flag to indicate whether the location supports creating new
 * tables. - default storage format for new tables created in this workspace.
 */

public class MPJdbcSchemaConfig {

  /** Default workspace is a root directory which supports read, but not write. */
  public static final MPJdbcSchemaConfig DEFAULT = new MPJdbcSchemaConfig("jdbc://", "",
      "");

  private final String uri;
  private final String username;
  private final String passwd;

  public MPJdbcSchemaConfig(@JsonProperty("uri") String uri,
      @JsonProperty("username") String username,
      @JsonProperty("passwd") String passwd) {
    this.uri = uri;
    this.username = username;
    this.passwd = passwd;
  }

  public String getUri() {
    return uri;
  }

  public boolean isWritable() {
    return false;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return passwd;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj == null || !(obj instanceof MPJdbcSchemaConfig)) {
      return false;
    }

    MPJdbcSchemaConfig that = (MPJdbcSchemaConfig) obj;
    return ((this.uri == null && that.uri == null) || this.uri.equals(that.uri))
        && this.passwd == that.passwd
        && ((this.username == null && that.username == null) || this.username
            .equals(that.username));
  }
}