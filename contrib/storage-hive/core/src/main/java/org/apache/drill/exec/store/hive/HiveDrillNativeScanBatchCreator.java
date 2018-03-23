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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Functions;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.hive.readers.HiveDefaultReader;
import org.apache.drill.exec.store.parquet.ParquetDirectByteBufferAllocator;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.util.Utilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("unused")
public class HiveDrillNativeScanBatchCreator implements BatchCreator<HiveDrillNativeParquetSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDrillNativeScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, HiveDrillNativeParquetSubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    final HiveTableWithColumnCache table = config.getTable();
    final List<List<InputSplit>> splits = config.getInputSplits();
    final List<HivePartition> partitions = config.getPartitions();
    final List<SchemaPath> columns = config.getColumns();
    final String partitionDesignator = context.getOptions()
        .getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    List<Map<String, String>> implicitColumns = Lists.newLinkedList();
    boolean selectAllQuery = Utilities.isStarQuery(columns);

    final boolean hasPartitions = (partitions != null && partitions.size() > 0);

    final List<String[]> partitionColumns = Lists.newArrayList();
    final List<Integer> selectedPartitionColumns = Lists.newArrayList();
    List<SchemaPath> tableColumns = columns;
    if (!selectAllQuery) {
      // Separate out the partition and non-partition columns. Non-partition columns are passed directly to the
      // ParquetRecordReader. Partition columns are passed to ScanBatch.
      tableColumns = Lists.newArrayList();
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        Matcher m = pattern.matcher(column.getRootSegmentPath());
        if (m.matches()) {
          selectedPartitionColumns.add(
              Integer.parseInt(column.getRootSegmentPath().substring(partitionDesignator.length())));
        } else {
          tableColumns.add(column);
        }
      }
    }

    final OperatorContext oContext = context.newOperatorContext(config);

    int currentPartitionIndex = 0;
    final List<RecordReader> readers = new LinkedList<>();

    final HiveConf conf = config.getHiveConf();

    // TODO: In future we can get this cache from Metadata cached on filesystem.
    final Map<String, ParquetMetadata> footerCache = Maps.newHashMap();

    Map<String, String> mapWithMaxColumns = Maps.newLinkedHashMap();
    try {
      for (List<InputSplit> splitGroups : splits) {
        for (InputSplit split : splitGroups) {
          final FileSplit fileSplit = (FileSplit) split;
          final Path finalPath = fileSplit.getPath();
          final JobConf cloneJob =
              new ProjectionPusher().pushProjectionsAndFilters(new JobConf(conf), finalPath.getParent());
          final FileSystem fs = finalPath.getFileSystem(cloneJob);

          ParquetMetadata parquetMetadata = footerCache.get(finalPath.toString());
          if (parquetMetadata == null) {
            parquetMetadata = ParquetFileReader.readFooter(cloneJob, finalPath);
            footerCache.put(finalPath.toString(), parquetMetadata);
          }
          final List<Integer> rowGroupNums = getRowGroupNumbersFromFileSplit(fileSplit, parquetMetadata);

          for (int rowGroupNum : rowGroupNums) {
            //DRILL-5009 : Skip the row group if the row count is zero
            if (parquetMetadata.getBlocks().get(rowGroupNum).getRowCount() == 0) {
              continue;
            }
            // Drill has only ever written a single row group per file, only detect corruption
            // in the first row group
            ParquetReaderUtility.DateCorruptionStatus containsCorruptDates =
                ParquetReaderUtility.detectCorruptDates(parquetMetadata, config.getColumns(), true);
            if (logger.isDebugEnabled()) {
              logger.debug(containsCorruptDates.toString());
            }
            readers.add(new ParquetRecordReader(
                context,
                Path.getPathWithoutSchemeAndAuthority(finalPath).toString(),
                rowGroupNum, fs,
                CodecFactory.createDirectCodecFactory(fs.getConf(),
                    new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
                parquetMetadata,
                tableColumns,
                containsCorruptDates)
            );
            Map<String, String> implicitValues = Maps.newLinkedHashMap();

            if (hasPartitions) {
              List<String> values = partitions.get(currentPartitionIndex).getValues();
              for (int i = 0; i < values.size(); i++) {
                if (selectAllQuery || selectedPartitionColumns.contains(i)) {
                  implicitValues.put(partitionDesignator + i, values.get(i));
                }
              }
            }
            implicitColumns.add(implicitValues);
            if (implicitValues.size() > mapWithMaxColumns.size()) {
              mapWithMaxColumns = implicitValues;
            }
          }
          currentPartitionIndex++;
        }
      }
    } catch (final IOException|RuntimeException e) {
      AutoCloseables.close(e, readers);
      throw new ExecutionSetupException("Failed to create RecordReaders. " + e.getMessage(), e);
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    mapWithMaxColumns = Maps.transformValues(mapWithMaxColumns, Functions.constant((String) null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, mapWithMaxColumns).entriesOnlyOnRight());
    }

    // If there are no readers created (which is possible when the table is empty or no row groups are matched),
    // create an empty RecordReader to output the schema
    if (readers.size() == 0) {
      readers.add(new HiveDefaultReader(table, null, null, tableColumns, context, conf,
        ImpersonationUtil.createProxyUgi(config.getUserName(), context.getQueryUserName())));
    }

    return new ScanBatch(context, oContext, readers, implicitColumns);
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
