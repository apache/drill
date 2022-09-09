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
package org.apache.drill.exec.store.jdbc;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.List;

public class JdbcInsertWriter extends JdbcWriter {
  public static final String OPERATOR_TYPE = "JDBC_INSERT_WRITER";

  @JsonCreator
  public JdbcInsertWriter(
    @JsonProperty("child") PhysicalOperator child,
    @JsonProperty("tableIdentifier") List<String> tableIdentifier,
    @JsonProperty("storage") StoragePluginConfig storageConfig,
    @JacksonInject JdbcSchema inner,
    @JacksonInject StoragePluginRegistry engineRegistry) {
    super(child, tableIdentifier, inner, engineRegistry.resolve(storageConfig, JdbcStoragePlugin.class));
  }

  JdbcInsertWriter(PhysicalOperator child, List<String> tableIdentifier, JdbcSchema inner, JdbcStoragePlugin plugin) {
    super(child, tableIdentifier, inner, plugin);
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new JdbcInsertWriter(child, getTableIdentifier(), getInner(), getPlugin());
  }

}
