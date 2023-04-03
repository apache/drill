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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Build the original scanner based on the {@link RecordReader} interface.
 * Requires that the storage plugin roll its own solutions for null columns.
 * Is not able to limit vector or batch sizes. Retained or backward
 * compatibility with "classic" format plugins which have not yet been
 * upgraded to use the new framework.
 */
public class ClassicScanBuilder {

  private final FragmentContext context;
  private EasySubScan scan;
  private final EasyFormatPlugin<? extends FormatPluginConfig> plugin;

  public ClassicScanBuilder(FragmentContext context, EasySubScan scan,
      EasyFormatPlugin<? extends FormatPluginConfig> plugin) {
    this.context = context;
    this.scan = scan;
    this.plugin = plugin;
  }

  public CloseableRecordBatch build() throws ExecutionSetupException {
    final ColumnExplorer columnExplorer =
        new ColumnExplorer(context.getOptions(), scan.getColumns());

    if (! columnExplorer.isStarQuery()) {
      scan = new EasySubScan(scan.getUserName(), scan.getWorkUnits(), scan.getFormatPlugin(),
          columnExplorer.getTableColumns(), scan.getSelectionRoot(),
          scan.getPartitionDepth(), scan.getSchema(), scan.getMaxRecords());
      scan.setOperatorId(scan.getOperatorId());
    }

    final OperatorContext oContext = context.newOperatorContext(scan);
    final DrillFileSystem dfs;
    try {
      dfs = oContext.newFileSystem(plugin.getFsConf());
    } catch (final IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create FileSystem: %s", e.getMessage()), e);
    }

    final List<RecordReader> readers = new LinkedList<>();
    final List<Map<String, String>> implicitColumns = Lists.newArrayList();
    Map<String, String> mapWithMaxColumns = Maps.newLinkedHashMap();
    final boolean supportsFileImplicitColumns = scan.getSelectionRoot() != null;
    for (final FileWork work : scan.getWorkUnits()) {
      final RecordReader recordReader = plugin.getRecordReader(
          context, dfs, work, scan.getColumns(), scan.getUserName());
      readers.add(recordReader);
      final List<String> partitionValues = ColumnExplorer.listPartitionValues(
          work.getPath(), scan.getSelectionRoot(), false);
      final Map<String, String> implicitValues = columnExplorer.populateColumns(
          work.getPath(), partitionValues, supportsFileImplicitColumns, dfs);
      implicitColumns.add(implicitValues);
      if (implicitValues.size() > mapWithMaxColumns.size()) {
        mapWithMaxColumns = implicitValues;
      }
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    final Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant(null));
    for (final Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(context, oContext, readers, implicitColumns);
  }
}
