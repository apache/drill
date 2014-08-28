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
package org.apache.drill.exec.store.easy.text;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Maps;
import com.google.common.base.Preconditions;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.text.DrillTextRecordReader;
import org.apache.drill.exec.store.text.DrillTextRecordWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TextFormatPlugin extends EasyFormatPlugin<TextFormatPlugin.TextFormatConfig> {

  public TextFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig) {
    super(name, context, fs, storageConfig, new TextFormatConfig(), true, false, true, true, new ArrayList<String>(), "text");
  }

  public TextFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig config, TextFormatConfig formatPluginConfig) {
    super(name, context, fs, config, formatPluginConfig, true, false, true, true, formatPluginConfig.getExtensions(), "text");
  }


  @Override
  public RecordReader getRecordReader(FragmentContext context, FileWork fileWork,
      List<SchemaPath> columns) throws ExecutionSetupException {
    Path path = getFileSystem().getUnderlying().makeQualified(new Path(fileWork.getPath()));
    FileSplit split = new FileSplit(path, fileWork.getStart(), fileWork.getLength(), new String[]{""});
    Preconditions.checkArgument(((TextFormatConfig)formatConfig).getDelimiter().length() == 1, "Only single character delimiter supported");
    return new DrillTextRecordReader(split, context, ((TextFormatConfig) formatConfig).getDelimiter().charAt(0), columns);
  }

  @Override
  public AbstractGroupScan getGroupScan(FileSelection selection, List<SchemaPath> columns) throws IOException {
    return new EasyGroupScan(selection, this, columns, selection.selectionRoot); //TODO : textformat supports project?
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("separator", ((TextFormatConfig)getConfig()).getDelimiter());
    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put("extension", ((TextFormatConfig)getConfig()).getExtensions().get(0));

    RecordWriter recordWriter = new DrillTextRecordWriter(context.getAllocator());
    recordWriter.init(options);

    return recordWriter;
  }

  @JsonTypeName("text")
  public static class TextFormatConfig implements FormatPluginConfig {

    public List<String> extensions;
    public String delimiter = "\n";

    public List<String> getExtensions() {
      return extensions;
    }

    public String getDelimiter() {
      return delimiter;
    }

    @Override
    public int hashCode() {
      return 33;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof TextFormatConfig))
        return false;
      TextFormatConfig that = (TextFormatConfig) obj;
      if (this.delimiter.equals(that.delimiter))
        return true;
      return false;
    }

  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.TEXT_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.TEXT_WRITER_VALUE;
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }
}
