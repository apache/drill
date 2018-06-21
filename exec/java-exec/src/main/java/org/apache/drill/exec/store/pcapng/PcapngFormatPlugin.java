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
package org.apache.drill.exec.store.pcapng;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class PcapngFormatPlugin extends EasyFormatPlugin<PcapngFormatConfig> {

  public static final String DEFAULT_NAME = "pcapng";

  public PcapngFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
                            StoragePluginConfig storagePluginConfig) {
    this(name, context, fsConf, storagePluginConfig, new PcapngFormatConfig());
  }

  public PcapngFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config, PcapngFormatConfig formatPluginConfig) {
    super(name, context, fsConf, config, formatPluginConfig, true,
        false, true, false,
        formatPluginConfig.getExtensions(), DEFAULT_NAME);
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs,
                                      FileWork fileWork, List<SchemaPath> columns,
                                      String userName) {
    return new PcapngRecordReader(fileWork.getPath(), dfs, columns);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.PCAPNG_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException("unimplemented");
  }
}
