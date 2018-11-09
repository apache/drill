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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

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

    private static final List<String> DEFAULT_EXTS = ImmutableList.of("mp");
    /**
     * List of file extensions this reader will handle.
     */
    public List<String> extensions = DEFAULT_EXTS;
    /**
     * When encontering corrupted messages in the msgpack files errors are thrown
     * and query processing stops. When lenient mode is turned on the error is
     * reported but not thrown which lets the querying process continue.
     */
    private boolean lenient = true;
    /**
     * In addition to logginer errors and warnings enabling this option will also
     * print errors to the sqlline console.
     */
    private boolean printToConsole = true;
    /**
     * When this option is enabled on the msgpack reader will "learn" the schema of
     * a msgpack file and write the information to disk.
     */
    private boolean learnSchema = true;
    /**
     * When this option is enabled the msgpack reader will use schema information
     * gathered during a learning phase. The schema helps the reader determine
     * missing columns and coercing data into the expected/desired type.
     */
    private boolean useSchema = true;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      if (extensions == null) {
        return DEFAULT_EXTS;
      }
      return extensions;
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public boolean isLenient() {
      return lenient;
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public boolean isPrintToConsole() {
      return printToConsole;
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public boolean isLearnSchema() {
      return learnSchema;
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public boolean isUseSchema() {
      return useSchema;
    }

    public void setUseSchema(boolean useSchema) {
      this.useSchema = useSchema;
    }

    public void setLearnSchema(boolean learnSchema) {
      this.learnSchema = learnSchema;
    }

    public void setExtensions(List<String> extensions) {
      this.extensions = extensions;
    }

    public void setLenient(boolean lenient) {
      this.lenient = lenient;
    }

    public void setPrintToConsole(boolean printToConsole) {
      this.printToConsole = printToConsole;
    }

    @Override
    public int hashCode() {
      return Objects.hash(extensions, learnSchema, printToConsole, lenient, useSchema);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final MsgpackFormatConfig other = (MsgpackFormatConfig) obj;
      return Objects.deepEquals(this.extensions, other.extensions)
          && Objects.equals(this.learnSchema, other.learnSchema) && Objects.equals(this.lenient, other.lenient)
          && Objects.equals(this.printToConsole, other.printToConsole)
          && Objects.equals(this.useSchema, other.useSchema);
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
