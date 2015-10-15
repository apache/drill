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
package org.apache.drill.exec.store.maprdb;

import static com.mapr.fs.jni.MapRConstants.MAPRFS_PREFIX;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;
import org.apache.drill.exec.store.maprdb.binary.BinaryTableGroupScan;
import org.apache.drill.exec.store.maprdb.binary.MapRDBPushFilterIntoScan;
import org.apache.drill.exec.store.maprdb.json.JsonTableGroupScan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.tables.TableProperties;

public class MapRDBFormatPlugin implements FormatPlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MapRDBFormatPlugin.class);

  private final FileSystemConfig storageConfig;
  private final MapRDBFormatPluginConfig config;
  private final MapRDBFormatMatcher matcher;
  private final Configuration fsConf;
  private final DrillbitContext context;
  private final String name;

  private volatile FileSystemPlugin storagePlugin;
  private volatile MapRFileSystem maprfs;

  public MapRDBFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new MapRDBFormatPluginConfig());
  }

  public MapRDBFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, MapRDBFormatPluginConfig formatConfig) {
    this.context = context;
    this.config = formatConfig;
    this.matcher = new MapRDBFormatMatcher(this);
    this.storageConfig = (FileSystemConfig) storageConfig;
    this.fsConf = fsConf;
    this.name = name == null ? "maprdb" : name;
    try {
      this.maprfs = new MapRFileSystem();
      maprfs.initialize(new URI(MAPRFS_PREFIX), fsConf);
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
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
  public boolean supportsAutoPartitioning() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  public Configuration getFsConf() {
    return fsConf;
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location,
      List<String> partitionColumns) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of(MapRDBPushFilterIntoScan.FILTER_ON_SCAN, MapRDBPushFilterIntoScan.FILTER_ON_PROJECT);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
      List<SchemaPath> columns) throws IOException {
    List<String> files = selection.getFiles();
    assert (files.size() == 1);
    String tableName = files.get(0);
    TableProperties props = maprfs.getTableProperties(new Path(tableName));

    if (props.getAttr().getJson()) {
      MapRDBSubScanSpec scanSpec = new MapRDBSubScanSpec().setTableName(tableName);
      return new JsonTableGroupScan(userName, getStoragePlugin(), this, scanSpec, columns);
    } else {
      HBaseScanSpec scanSpec = new HBaseScanSpec(tableName);
      return new BinaryTableGroupScan(userName, getStoragePlugin(), this, scanSpec, columns);
    }
  }

  @Override
  public FormatPluginConfig getConfig() {
    return config;
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
  }

  @Override
  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public String getName() {
    return name;
  }

  public synchronized FileSystemPlugin getStoragePlugin() {
    if (this.storagePlugin == null) {
      try {
        this.storagePlugin = (FileSystemPlugin) (context.getStorage().getPlugin(storageConfig));
      } catch (ExecutionSetupException e) {
        throw new RuntimeException(e);
      }
    }
    return storagePlugin;
  }

}
