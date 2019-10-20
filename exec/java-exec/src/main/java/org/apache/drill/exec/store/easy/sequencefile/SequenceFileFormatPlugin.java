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
package org.apache.drill.exec.store.easy.sequencefile;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.common.DrillStatsTable.TableStatistics;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class SequenceFileFormatPlugin extends EasyFormatPlugin<SequenceFileFormatConfig> {
  public SequenceFileFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
                                  StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new SequenceFileFormatConfig());
  }

  public SequenceFileFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
                                  StoragePluginConfig storageConfig, SequenceFileFormatConfig formatConfig) {
    super(name, context, fsConf, storageConfig, formatConfig,
      true, false, /* splittable = */ true, /* compressible = */ true,
      formatConfig.getExtensions(), "sequencefile");
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns)
    throws IOException {
    return new EasyGroupScan(userName, selection, this, columns, selection.selectionRoot, null);
  }

  @Override
  public boolean supportsStatistics() {
    return false;
  }

  @Override
  public TableStatistics readStatistics(FileSystem fs, Path statsTablePath) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public void writeStatistics(TableStatistics statistics, FileSystem fs, Path statsTablePath) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context,
                                      DrillFileSystem dfs,
                                      FileWork fileWork,
                                      List<SchemaPath> columns,
                                      String userName) throws ExecutionSetupException {
    final Path path = dfs.makeQualified(fileWork.getPath());
    final FileSplit split = new FileSplit(path, fileWork.getStart(), fileWork.getLength(), new String[]{""});
    return new SequenceFileRecordReader(split, dfs, context.getQueryUserName(), userName);
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.SEQUENCE_SUB_SCAN_VALUE;
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }
}