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

package org.apache.drill.exec.store.sentinel;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class SentinelRootSchema extends AbstractSchema {
  private final SentinelStoragePlugin plugin;
  private final Map<String, SentinelSchema> workspaceSchemas;

  public SentinelRootSchema(SentinelStoragePlugin plugin) {
    super(Collections.emptyList(), plugin.getName());
    this.plugin = plugin;
    this.workspaceSchemas = CaseInsensitiveMap.newHashMap();
  }

  @Override
  public SchemaPlus getSubSchema(String name) {
    if (!workspaceSchemas.containsKey(name)) {
      SentinelSchema schema = new SentinelSchema(plugin, name, name);
      workspaceSchemas.put(name, schema);
    }
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.copyOf(plugin.getConfig().getWorkspaceIds());
  }

  @Override
  public String getTypeName() {
    return "sentinel";
  }
}
