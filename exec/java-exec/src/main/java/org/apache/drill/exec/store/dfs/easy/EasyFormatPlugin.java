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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.QueryOptimizerRule;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

import com.beust.jcommander.internal.Lists;

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> implements FormatPlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  private final BasicFormatMatcher matcher;
  private final DrillbitContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  private final DrillFileSystem fs;
  private final StoragePluginConfig storageConfig;
  private final FormatPluginConfig formatConfig;
  private final String name;
  
  protected EasyFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig,
                             T formatConfig, boolean readable, boolean writable, boolean blockSplittable, String extension, String defaultName){
    this.matcher = new BasicFormatMatcher(this, fs, extension);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.fs = fs;
    this.storageConfig = storageConfig;
    this.formatConfig = formatConfig;
    this.name = name == null ? defaultName : name; 
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

  /**
   * Whether or not you can split the format based on blocks within file boundaries. If not, the simple format engine will
   * only split on file boundaries.
   * 
   * @return True if splittable.
   */
  public boolean isBlockSplittable(){
    return blockSplittable;
  };

  public abstract RecordReader getRecordReader(FragmentContext context, FileWork fileWork, FieldReference ref, List<SchemaPath> columns) throws ExecutionSetupException;

  
  RecordBatch getBatch(FragmentContext context, EasySubScan scan) throws ExecutionSetupException {
    List<RecordReader> readers = Lists.newArrayList();
    for(FileWork work : scan.getWorkUnits()){
      readers.add(getRecordReader(context, work, scan.getRef(), scan.getColumns())); 
    }
    
    return new ScanBatch(context, readers.iterator());
  }
  
  @Override
  public AbstractGroupScan getGroupScan(FieldReference outputRef, FileSelection selection) throws IOException {
    return new EasyGroupScan(selection, this, outputRef, null);
  }

  @Override
  public FormatPluginConfig getConfig() {
    return formatConfig;
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
  }

  @Override
  public boolean supportsRead() {
    return readable;
  }

  @Override
  public boolean supportsWrite() {
    return writable;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  @Override
  public List<QueryOptimizerRule> getOptimizerRules() {
    return Collections.emptyList();
  }

  
}
