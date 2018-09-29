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
package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class MsgpackFormatPlugin extends EasyFormatPlugin<MsgpackFormatConfig> {

  private static final boolean IS_COMPRESSIBLE = true;
  public static final String DEFAULT_NAME = "msgpack";

  public MsgpackFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new MsgpackFormatConfig());
  }

  public MsgpackFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config,
      MsgpackFormatConfig formatPluginConfig) {
    super(name, context, fsConf, config, formatPluginConfig, true, false, false, IS_COMPRESSIBLE,
        formatPluginConfig.getExtensions(), DEFAULT_NAME);
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
      List<SchemaPath> columns, String userName) {
    return new MsgpackRecordReader(getConfig(), context, fileWork.getPath(), dfs, columns);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    Map<String, String> options = new HashMap<>();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("extension", "mp");
    RecordWriter recordWriter = new MsgpackRecordWriter(writer.getStorageStrategy());
    recordWriter.init(options);

    return recordWriter;
  }

  @JsonTypeName("msgpack")
  public static class MsgpackFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of("mp");
    public boolean skipMalformedMsgRecords = true;
    public boolean printSkippedMalformedMsgRecordLineNumber = true;
    private static final List<String> DEFAULT_EXTS = ImmutableList.of("mp");

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      if (extensions == null) {
        // when loading an old JSONFormatConfig that doesn't contain an
        // "extensions"
        // attribute
        return DEFAULT_EXTS;
      }
      return extensions;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
      result = prime * result + (printSkippedMalformedMsgRecordLineNumber ? 1231 : 1237);
      result = prime * result + (skipMalformedMsgRecords ? 1231 : 1237);
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
      MsgpackFormatConfig other = (MsgpackFormatConfig) obj;
      if (extensions == null) {
        if (other.extensions != null) {
          return false;
        }
      } else if (!extensions.equals(other.extensions)) {
        return false;
      }
      if (printSkippedMalformedMsgRecordLineNumber != other.printSkippedMalformedMsgRecordLineNumber) {
        return false;
      }
      if (skipMalformedMsgRecords != other.skipMalformedMsgRecords) {
        return false;
      }
      return true;
    }

    public boolean isSkipMalformedMsgRecords() {
      return skipMalformedMsgRecords;
    }

    public boolean isPrintSkippedMalformedMsgRecordLineNumber() {
      return printSkippedMalformedMsgRecordLineNumber;
    }
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.JSON_WRITER_VALUE;
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }
}
