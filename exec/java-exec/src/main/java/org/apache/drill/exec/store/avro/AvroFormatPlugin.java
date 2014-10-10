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
package org.apache.drill.exec.store.avro;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableSet;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Format plugin for Avro data files.
 */
public class AvroFormatPlugin implements FormatPlugin {

  private final String name;
  private final DrillbitContext context;
  private final DrillFileSystem fs;
  private final StoragePluginConfig storagePluginConfig;
  private final AvroFormatConfig formatConfig;
  private final BasicFormatMatcher matcher;

  public AvroFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs,
                          StoragePluginConfig storagePluginConfig) {
    this(name, context, fs, storagePluginConfig, new AvroFormatConfig());
  }

  public AvroFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs,
                          StoragePluginConfig storagePluginConfig, AvroFormatConfig formatConfig) {
    this.name = name;
    this.context = context;
    this.fs = fs;
    this.storagePluginConfig = storagePluginConfig;
    this.formatConfig = formatConfig;

    // XXX - What does 'compressible' mean in this context?
    this.matcher = new BasicFormatMatcher(this, fs, Lists.newArrayList("avro"), false);
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  @Override
  public AbstractWriter getWriter(final PhysicalOperator child, final String location) throws IOException {
    throw new UnsupportedOperationException("Unimplemented");
  }

  @Override
  public AbstractGroupScan getGroupScan(final FileSelection selection) throws IOException {
    return new AvroGroupScan(selection.getFileStatusList(fs), this, selection.selectionRoot, null);
  }

  @Override
  public AbstractGroupScan getGroupScan(final FileSelection selection, final List<SchemaPath> columns) throws IOException {
    return new AvroGroupScan(selection.getFileStatusList(fs), this, selection.selectionRoot, columns);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of();
  }

  @Override
  public AvroFormatConfig getConfig() {
    return formatConfig;
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storagePluginConfig;
  }

  @Override
  public DrillFileSystem getFileSystem() {
    return fs;
  }

  @Override
  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public String getName() {
    return name;
  }
}
