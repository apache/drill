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
package org.apache.drill.exec.store.easy.json;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

public class JSONFormatPlugin extends EasyFormatPlugin<JSONFormatConfig> {

  public JSONFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig) {
    this(name, context, fs, storageConfig, new JSONFormatConfig());
  }
  
  public JSONFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig config, JSONFormatConfig formatPluginConfig) {
    super(name, context, fs, config, formatPluginConfig, true, false, false, "json", "json");
  }
  
  @Override
  public RecordReader getRecordReader(FragmentContext context, FileWork fileWork, FieldReference ref,
      List<SchemaPath> columns) throws ExecutionSetupException {
    return new JSONRecordReader(context, fileWork.getPath(), this.getFileSystem().getUnderlying(), ref, columns);
  }

  @JsonTypeName("json")
  public static class JSONFormatConfig implements FormatPluginConfig {

    @Override
    public int hashCode() {
      return 31;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() == obj.getClass())
        return true;
      return false;
    }
    
  }
}
