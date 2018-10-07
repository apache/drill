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
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FormatMatcher;
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
  public FormatMatcher getMatcher() {
    return super.getMatcher();
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
      List<SchemaPath> columns, String userName) {
    return new MsgpackRecordReader(getConfig(), context, fileWork.getPath(), dfs, columns);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @JsonTypeName("msgpack")
  public static class MsgpackFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of("mp");
    private boolean skipMalformedMsgRecords = true;
    private boolean printSkippedMalformedMsgRecordLineNumber = true;
    private boolean learnSchema = true;
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

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isSkipMalformedMsgRecords() {
      return skipMalformedMsgRecords;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isPrintSkippedMalformedMsgRecordLineNumber() {
      return printSkippedMalformedMsgRecordLineNumber;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isLearnSchema() {
      return learnSchema;
    }

    public void setLearnSchema(boolean learnSchema) {
      this.learnSchema = learnSchema;
    }

    public void setExtensions(List<String> extensions) {
      this.extensions = extensions;
    }

    public void setSkipMalformedMsgRecords(boolean skipMalformedMsgRecords) {
      this.skipMalformedMsgRecords = skipMalformedMsgRecords;
    }

    public void setPrintSkippedMalformedMsgRecordLineNumber(boolean printSkippedMalformedMsgRecordLineNumber) {
      this.printSkippedMalformedMsgRecordLineNumber = printSkippedMalformedMsgRecordLineNumber;
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
