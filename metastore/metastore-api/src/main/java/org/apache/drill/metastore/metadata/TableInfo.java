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
package org.apache.drill.metastore.metadata;

/**
 * General table information.
 */
public class TableInfo {
  public static final String UNKNOWN = "UNKNOWN";
  public static final TableInfo UNKNOWN_TABLE_INFO = TableInfo.builder()
      .storagePlugin(UNKNOWN)
      .workspace(UNKNOWN)
      .name(UNKNOWN)
      .type(UNKNOWN)
      .owner(UNKNOWN)
      .build();

  private final String storagePlugin;
  private final String workspace;
  private final String name;
  private final String type;
  private final String owner;

  private TableInfo(TableInfoBuilder builder) {
    this.storagePlugin = builder.storagePlugin;
    this.workspace = builder.workspace;
    this.name = builder.name;
    this.type = builder.type;
    this.owner = builder.owner;
  }

  public String getStoragePlugin() {
    return storagePlugin;
  }

  public String getWorkspace() {
    return workspace;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getOwner() {
    return owner;
  }

  public static TableInfoBuilder builder() {
    return new TableInfoBuilder();
  }

  public static class TableInfoBuilder {
    private String storagePlugin;
    private String workspace;
    private String name;
    private String type;
    private String owner;

    public TableInfoBuilder storagePlugin(String storagePlugin) {
      this.storagePlugin = storagePlugin;
      return this;
    }

    public TableInfoBuilder workspace(String workspace) {
      this.workspace = workspace;
      return this;
    }

    public TableInfoBuilder name(String name) {
      this.name = name;
      return this;
    }

    public TableInfoBuilder type(String type) {
      this.type = type;
      return this;
    }

    public TableInfoBuilder owner(String owner) {
      this.owner = owner;
      return this;
    }

    public TableInfo build() {
      return new TableInfo(this);
    }

  }
}
