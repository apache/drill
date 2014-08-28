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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableSet;

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> implements FormatPlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  private final BasicFormatMatcher matcher;
  private final DrillbitContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  private final DrillFileSystem fs;
  private final StoragePluginConfig storageConfig;
  protected final FormatPluginConfig formatConfig;
  private final String name;
  protected final CompressionCodecFactory codecFactory;
  private final boolean compressible;

  protected EasyFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig,
                             T formatConfig, boolean readable, boolean writable, boolean blockSplittable, boolean compressible, List<String> extensions, String defaultName){
    this.matcher = new BasicFormatMatcher(this, fs, extensions, compressible);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.compressible = compressible;
    this.fs = fs;
    this.storageConfig = storageConfig;
    this.formatConfig = formatConfig;
    this.name = name == null ? defaultName : name;
    this.codecFactory = new CompressionCodecFactory(new Configuration(fs.getUnderlying().getConf()));
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

  public abstract boolean supportsPushDown();

  /**
   * Whether or not you can split the format based on blocks within file boundaries. If not, the simple format engine will
   * only split on file boundaries.
   *
   * @return True if splittable.
   */
  public boolean isBlockSplittable() {
    return blockSplittable;
  }

  public boolean isCompressible() {
    return compressible;
  }

  public abstract RecordReader getRecordReader(FragmentContext context, FileWork fileWork, List<SchemaPath> columns) throws ExecutionSetupException;

  RecordBatch getReaderBatch(FragmentContext context, EasySubScan scan) throws ExecutionSetupException {
    String partitionDesignator = context.getConfig().getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    List<SchemaPath> columns = scan.getColumns();
    List<RecordReader> readers = Lists.newArrayList();
    List<String[]> partitionColumns = Lists.newArrayList();
    List<Integer> selectedPartitionColumns = Lists.newArrayList();
    boolean selectAllColumns = false;

    if (columns == null || columns.size() == 0) {
      selectAllColumns = true;
    } else {
      List<SchemaPath> newColumns = Lists.newArrayList();
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        Matcher m = pattern.matcher(column.getAsUnescapedPath());
        if (m.matches()) {
          selectedPartitionColumns.add(Integer.parseInt(column.getAsUnescapedPath().toString().substring(partitionDesignator.length())));
        } else {
          newColumns.add(column);
        }
      }
      // Create a new sub scan object with the new set of columns;
      scan = new EasySubScan(scan.getWorkUnits(), scan.getFormatPlugin(), newColumns, scan.getSelectionRoot());
    }

    int numParts = 0;
    for(FileWork work : scan.getWorkUnits()){
      readers.add(getRecordReader(context, work, scan.getColumns()));
      if (scan.getSelectionRoot() != null) {
        String[] r = scan.getSelectionRoot().split("/");
        String[] p = work.getPath().split("/");
        if (p.length > r.length) {
          String[] q = ArrayUtils.subarray(p, r.length, p.length - 1);
          partitionColumns.add(q);
          numParts = Math.max(numParts, q.length);
        } else {
          partitionColumns.add(new String[] {});
        }
      } else {
        partitionColumns.add(new String[] {});
      }
    }

    if (selectAllColumns) {
      for (int i = 0; i < numParts; i++) {
        selectedPartitionColumns.add(i);
      }
    }

    return new ScanBatch(scan, context, readers.iterator(), partitionColumns, selectedPartitionColumns);
  }

  public abstract RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException;

  public RecordBatch getWriterBatch(FragmentContext context, RecordBatch incoming, EasyWriter writer)
      throws ExecutionSetupException {
    try {
      return new WriterRecordBatch(writer, incoming, context, getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location) throws IOException {
    return new EasyWriter(child, location, this);
  }

  @Override
  public AbstractGroupScan getGroupScan(FileSelection selection) throws IOException {
    return new EasyGroupScan(selection, this, selection.selectionRoot);
  }

  @Override
  public AbstractGroupScan getGroupScan(FileSelection selection, List<SchemaPath> columns) throws IOException {
    return new EasyGroupScan(selection, this, columns, selection.selectionRoot);
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
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of();
  }

  public abstract int getReaderOperatorType();
  public abstract int getWriterOperatorType();

}
