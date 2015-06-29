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
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> implements FormatPlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  private final BasicFormatMatcher matcher;
  private final DrillbitContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  private final Configuration fsConf;
  private final StoragePluginConfig storageConfig;
  protected final FormatPluginConfig formatConfig;
  private final String name;
  private final boolean compressible;

  protected EasyFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, T formatConfig, boolean readable, boolean writable, boolean blockSplittable,
      boolean compressible, List<String> extensions, String defaultName){
    this.matcher = new BasicFormatMatcher(this, fsConf, extensions, compressible);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.compressible = compressible;
    this.fsConf = fsConf;
    this.storageConfig = storageConfig;
    this.formatConfig = formatConfig;
    this.name = name == null ? defaultName : name;
  }

  @Override
  public Configuration getFsConf() {
    return fsConf;
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

  public abstract RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
      List<SchemaPath> columns) throws ExecutionSetupException;

  CloseableRecordBatch getReaderBatch(FragmentContext context, EasySubScan scan) throws ExecutionSetupException {
    String partitionDesignator = context.getOptions()
      .getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    List<SchemaPath> columns = scan.getColumns();
    List<RecordReader> readers = Lists.newArrayList();
    List<String[]> partitionColumns = Lists.newArrayList();
    List<Integer> selectedPartitionColumns = Lists.newArrayList();
    boolean selectAllColumns = false;

    if (columns == null || columns.size() == 0 || AbstractRecordReader.isStarQuery(columns)) {
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

      // We must make sure to pass a table column(not to be confused with partition column) to the underlying record
      // reader.
      if (newColumns.size()==0) {
        newColumns.add(AbstractRecordReader.STAR_COLUMN);
      }
      // Create a new sub scan object with the new set of columns;
      EasySubScan newScan = new EasySubScan(scan.getUserName(), scan.getWorkUnits(), scan.getFormatPlugin(),
          newColumns, scan.getSelectionRoot());
      newScan.setOperatorId(scan.getOperatorId());
      scan = newScan;
    }

    int numParts = 0;
    OperatorContext oContext = context.newOperatorContext(scan, false /*
                                                                       * ScanBatch is not subject to fragment memory
                                                                       * limit
                                                                       */);
    final DrillFileSystem dfs;
    try {
      dfs = oContext.newFileSystem(fsConf);
    } catch (IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create FileSystem: %s", e.getMessage()), e);
    }

    for(FileWork work : scan.getWorkUnits()){
      readers.add(getRecordReader(context, dfs, work, scan.getColumns()));
      if (scan.getSelectionRoot() != null) {
        String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(scan.getSelectionRoot())).toString().split("/");
        String[] p = Path.getPathWithoutSchemeAndAuthority(new Path(work.getPath())).toString().split("/");
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

    return new ScanBatch(scan, context, oContext, readers.iterator(), partitionColumns, selectedPartitionColumns);
  }

  public abstract RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException;

  public CloseableRecordBatch getWriterBatch(FragmentContext context, RecordBatch incoming, EasyWriter writer)
      throws ExecutionSetupException {
    try {
      return new WriterRecordBatch(writer, incoming, context, getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  protected ScanStats getScanStats(final PlannerSettings settings, final EasyGroupScan scan) {
    long data = 0;
    for (final CompleteFileWork work : scan.getWorkIterable()) {
      data += work.getTotalBytes();
    }

    final long estRowCount = data / 1024;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, data);
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location, List<String> partitionColumns) throws IOException {
    return new EasyWriter(child, location, partitionColumns, this);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns)
      throws IOException {
    return new EasyGroupScan(userName, selection, this, columns, selection.selectionRoot);
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
  public boolean supportsAutoPartitioning() {
    return false;
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
