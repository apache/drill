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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.parquet.DirectCodecFactory;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

@SuppressWarnings("unused")
public class HiveDrillNativeScanBatchCreator implements BatchCreator<HiveDrillNativeParquetSubScan> {

  @Override
  public ScanBatch getBatch(FragmentContext context, HiveDrillNativeParquetSubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    final Table table = config.getTable();
    final List<InputSplit> splits = config.getInputSplits();
    final List<Partition> partitions = config.getPartitions();
    final List<SchemaPath> columns = config.getColumns();
    final Map<String, String> hiveConfigOverride = config.getHiveReadEntry().hiveConfigOverride;
    final String partitionDesignator = context.getOptions()
        .getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;

    final boolean hasPartitions = (partitions != null && partitions.size() > 0);

    final List<String[]> partitionColumns = Lists.newArrayList();
    final List<Integer> selectedPartitionColumns = Lists.newArrayList();
    List<SchemaPath> newColumns = columns;
    if (AbstractRecordReader.isStarQuery(columns)) {
      for (int i = 0; i < table.getPartitionKeys().size(); i++) {
        selectedPartitionColumns.add(i);
      }
    } else {
      // Separate out the partition and non-partition columns. Non-partition columns are passed directly to the
      // ParquetRecordReader. Partition columns are passed to ScanBatch.
      newColumns = Lists.newArrayList();
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        Matcher m = pattern.matcher(column.getAsUnescapedPath());
        if (m.matches()) {
          selectedPartitionColumns.add(
              Integer.parseInt(column.getAsUnescapedPath().toString().substring(partitionDesignator.length())));
        } else {
          newColumns.add(column);
        }
      }
    }

    final OperatorContext oContext = context.newOperatorContext(config,
        false /* ScanBatch is not subject to fragment memory limit */);

    int currentPartitionIndex = 0;
    boolean success = false;
    final List<RecordReader> readers = Lists.newArrayList();

    final Configuration conf = getConf(hiveConfigOverride);

    // TODO: In future we can get this cache from Metadata cached on filesystem.
    final Map<String, ParquetMetadata> footerCache = Maps.newHashMap();

    try {
      for (InputSplit split : splits) {
        final FileSplit fileSplit = (FileSplit) split;
        final Path finalPath = fileSplit.getPath();
        final JobConf cloneJob =
            new ProjectionPusher().pushProjectionsAndFilters(new JobConf(conf), finalPath.getParent());
        final FileSystem fs = finalPath.getFileSystem(cloneJob);

        ParquetMetadata parquetMetadata = footerCache.get(finalPath.toString());
        if (parquetMetadata == null){
          parquetMetadata = ParquetFileReader.readFooter(cloneJob, finalPath);
          footerCache.put(finalPath.toString(), parquetMetadata);
        }
        final List<Integer> rowGroupNums = getRowGroupNumbersFromFileSplit(fileSplit, parquetMetadata);

        for(int rowGroupNum : rowGroupNums) {
          readers.add(new ParquetRecordReader(
                  context,
                  Path.getPathWithoutSchemeAndAuthority(finalPath).toString(),
                  rowGroupNum, fs,
                  new DirectCodecFactory(fs.getConf(), oContext.getAllocator()),
                  parquetMetadata,
                  newColumns)
          );

          if (hasPartitions) {
            Partition p = partitions.get(currentPartitionIndex);
            partitionColumns.add(p.getValues().toArray(new String[0]));
          }
        }
        currentPartitionIndex++;
      }
      success = true;
    } catch (final IOException e) {
      throw new ExecutionSetupException("Failed to create RecordReaders. " + e.getMessage(), e);
    } finally {
      if (!success) {
        for(RecordReader reader : readers) {
          AutoCloseables.close(reader, logger);
        }
      }
    }

    // If there are no readers created (which is possible when the table is empty or no row groups are matched),
    // create an empty RecordReader to output the schema
    if (readers.size() == 0) {
      readers.add(new HiveRecordReader(table, null, null, columns, context, hiveConfigOverride,
        ImpersonationUtil.createProxyUgi(config.getUserName(), context.getQueryUserName())));
    }

    return new ScanBatch(config, context, oContext, readers.iterator(), partitionColumns, selectedPartitionColumns);
  }

  private Configuration getConf(final Map<String, String> hiveConfigOverride) {
    final HiveConf hiveConf = new HiveConf();
    for(Entry<String, String> prop : hiveConfigOverride.entrySet()) {
      hiveConf.set(prop.getKey(), prop.getValue());
    }

    return hiveConf;
  }

  /**
   * Get the list of row group numbers for given file input split. Logic used here is same as how Hive's parquet input
   * format finds the row group numbers for input split.
   */
  private List<Integer> getRowGroupNumbersFromFileSplit(final FileSplit split,
      final ParquetMetadata footer) throws IOException {
    final List<BlockMetaData> blocks = footer.getBlocks();

    final long splitStart = split.getStart();
    final long splitLength = split.getLength();

    final List<Integer> rowGroupNums = Lists.newArrayList();

    int i = 0;
    for (final BlockMetaData block : blocks) {
      final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
      if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
        rowGroupNums.add(i);
      }
      i++;
    }

    return rowGroupNums;
  }
}
