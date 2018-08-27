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

package org.apache.drill.exec.store.log;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
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

public class LogFormatPlugin extends EasyFormatPlugin<LogFormatConfig> {

  public static final String DEFAULT_NAME = "logRegex";
  private final LogFormatConfig formatConfig;

  public LogFormatPlugin(String name, DrillbitContext context,
                         Configuration fsConf, StoragePluginConfig storageConfig,
                         LogFormatConfig formatConfig) {
    super(name, context, fsConf, storageConfig, formatConfig,
        true,  // readable
        false, // writable
        true, // blockSplittable
        true,  // compressible
        Lists.newArrayList(formatConfig.getExtension()),
        DEFAULT_NAME);
    this.formatConfig = formatConfig;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context,
                                      DrillFileSystem dfs, FileWork fileWork, List<SchemaPath> columns,
                                      String userName) throws ExecutionSetupException {
    return new LogRecordReader(context, dfs, fileWork,
        columns, userName, formatConfig);
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context,
                                      EasyWriter writer) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.REGEX_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException("unimplemented");
  }
}
