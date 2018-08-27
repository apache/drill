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
package org.apache.drill.exec.store.pcap;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class PcapFormatPlugin extends EasyFormatPlugin<PcapFormatConfig> {

  private final PcapFormatMatcher matcher;

  public PcapFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
                          StoragePluginConfig storagePluginConfig) {
    this(name, context, fsConf, storagePluginConfig, new PcapFormatConfig());
  }

  public PcapFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config, PcapFormatConfig formatPluginConfig) {
    super(name, context, fsConf, config, formatPluginConfig, true, false, true, false, Lists.newArrayList("pcap"), "pcap");
    this.matcher = new PcapFormatMatcher(this);
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork, List<SchemaPath> columns, String userName) throws ExecutionSetupException {
    return new PcapRecordReader(fileWork.getPath(), dfs, columns);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.PCAP_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public FormatMatcher getMatcher() {
    return this.matcher;
  }

  private static class PcapFormatMatcher extends BasicFormatMatcher {

    public PcapFormatMatcher(PcapFormatPlugin plugin) {
      super(plugin, ImmutableList.of(Pattern.compile(".*\\.pcap$")), ImmutableList.<MagicString>of());
    }

    @Override
    public DrillTable isReadable(DrillFileSystem fs,
                                 FileSelection selection, FileSystemPlugin fsPlugin,
                                 String storageEngineName, SchemaConfig schemaConfig) throws IOException {
      if (isFileReadable(fs, selection.getFirstPath(fs))) {
        return new PcapDrillTable(storageEngineName, fsPlugin, schemaConfig.getUserName(),
            new FormatSelection(plugin.getConfig(), selection));
      }
      return null;
    }
  }

}
