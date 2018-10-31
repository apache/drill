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
package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.hadoop.conf.Configuration;

/**
 * Similar to a storage engine but built specifically to work within a FileSystem context.
 */
public interface FormatPlugin {

  boolean supportsRead();

  boolean supportsWrite();

  /**
   * Indicates whether this FormatPlugin supports auto-partitioning for CTAS statements
   * @return true if auto-partitioning is supported
   */
  boolean supportsAutoPartitioning();

  FormatMatcher getMatcher();

  AbstractWriter getWriter(PhysicalOperator child, String location, List<String> partitionColumns) throws IOException;

  Set<StoragePluginOptimizerRule> getOptimizerRules();

  AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns) throws IOException;

  default AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns, OptionManager options) throws IOException {
    return getGroupScan(userName, selection, columns);
  }

  FormatPluginConfig getConfig();
  StoragePluginConfig getStorageConfig();
  Configuration getFsConf();
  DrillbitContext getContext();
  String getName();

}
