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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import org.apache.hadoop.fs.FileSystem;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JSONFormatPlugin extends EasyFormatPlugin<JSONFormatConfig> {

  private static final boolean IS_COMPRESSIBLE = true;

  public JSONFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig) {
    this(name, context, fs, storageConfig, new JSONFormatConfig());
  }

  public JSONFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig config, JSONFormatConfig formatPluginConfig) {
    super(name, context, fs, config, formatPluginConfig, true, false, false, IS_COMPRESSIBLE, Lists.newArrayList("json"), "json");
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
      List<SchemaPath> columns) throws ExecutionSetupException {
    return new JSONRecordReader(context, fileWork.getPath(), dfs, columns);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("separator", " ");
    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put("extension", "json");

    RecordWriter recordWriter = new JsonRecordWriter();
    recordWriter.init(options);

    return recordWriter;
  }

  @JsonTypeName("json")
  public static class JSONFormatConfig implements FormatPluginConfig {

    @Override
    public int hashCode() {
      return 31;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (getClass() == obj.getClass()) {
        return true;
      }
      return false;
    }

  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

}
