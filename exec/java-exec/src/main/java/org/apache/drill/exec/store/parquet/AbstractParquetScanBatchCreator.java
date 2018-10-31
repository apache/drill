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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.shaded.guava.com.google.common.base.Functions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.store.parquet2.DrillParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractParquetScanBatchCreator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetScanBatchCreator.class);

  protected ScanBatch getBatch(ExecutorFragmentContext context, AbstractParquetRowGroupScan rowGroupScan, OperatorContext oContext) throws ExecutionSetupException {
    final ColumnExplorer columnExplorer = new ColumnExplorer(context.getOptions(), rowGroupScan.getColumns());

    if (!columnExplorer.isStarQuery()) {
      rowGroupScan = rowGroupScan.copy(columnExplorer.getTableColumns());
      rowGroupScan.setOperatorId(rowGroupScan.getOperatorId());
    }

    AbstractDrillFileSystemManager fsManager = getDrillFileSystemCreator(oContext, context.getOptions());

    // keep footers in a map to avoid re-reading them
    Map<String, ParquetMetadata> footers = new HashMap<>();
    List<RecordReader> readers = new LinkedList<>();
    List<Map<String, String>> implicitColumns = new ArrayList<>();
    Map<String, String> mapWithMaxColumns = new LinkedHashMap<>();
    for (RowGroupReadEntry rowGroup : rowGroupScan.getRowGroupReadEntries()) {
      /*
      Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      try {
        Stopwatch timer = logger.isTraceEnabled() ? Stopwatch.createUnstarted() : null;
        DrillFileSystem fs = fsManager.get(rowGroupScan.getFsConf(rowGroup), rowGroup.getPath());
        ParquetReaderConfig readerConfig = rowGroupScan.getReaderConfig();
        if (!footers.containsKey(rowGroup.getPath())) {
          if (timer != null) {
            timer.start();
          }

          ParquetMetadata footer = readFooter(fs.getConf(), rowGroup.getPath(), readerConfig);
          if (timer != null) {
            long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
            logger.trace("ParquetTrace,Read Footer,{},{},{},{},{},{},{}", "", rowGroup.getPath(), "", 0, 0, 0, timeToRead);
          }
          footers.put(rowGroup.getPath(), footer);
        }
        ParquetMetadata footer = footers.get(rowGroup.getPath());

        ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(footer,
          rowGroupScan.getColumns(), readerConfig.autoCorrectCorruptedDates());
        logger.debug("Contains corrupt dates: {}.", containsCorruptDates);

        boolean useNewReader = context.getOptions().getBoolean(ExecConstants.PARQUET_NEW_RECORD_READER);
        boolean containsComplexColumn = ParquetReaderUtility.containsComplexColumn(footer, rowGroupScan.getColumns());
        logger.debug("PARQUET_NEW_RECORD_READER is {}. Complex columns {}.", useNewReader ? "enabled" : "disabled",
            containsComplexColumn ? "found." : "not found.");
        RecordReader reader;

        if (useNewReader || containsComplexColumn) {
          reader = new DrillParquetReader(context,
              footer,
              rowGroup,
              columnExplorer.getTableColumns(),
              fs,
              containsCorruptDates);
        } else {
          reader = new ParquetRecordReader(context,
              rowGroup.getPath(),
              rowGroup.getRowGroupIndex(),
              rowGroup.getNumRecordsToRead(),
              fs,
              CodecFactory.createDirectCodecFactory(fs.getConf(), new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
              footer,
              rowGroupScan.getColumns(),
              containsCorruptDates);
        }

        logger.debug("Query {} uses {}",
            QueryIdHelper.getQueryId(oContext.getFragmentContext().getHandle().getQueryId()),
            reader.getClass().getSimpleName());
        readers.add(reader);

        List<String> partitionValues = rowGroupScan.getPartitionValues(rowGroup);
        Map<String, String> implicitValues = columnExplorer.populateImplicitColumns(rowGroup.getPath(), partitionValues, rowGroupScan.supportsFileImplicitColumns());
        implicitColumns.add(implicitValues);
        if (implicitValues.size() > mapWithMaxColumns.size()) {
          mapWithMaxColumns = implicitValues;
        }

      } catch (IOException e) {
        throw new ExecutionSetupException(e);
      }
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant(null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(context, oContext, readers, implicitColumns);
  }

  protected abstract AbstractDrillFileSystemManager getDrillFileSystemCreator(OperatorContext operatorContext, OptionManager optionManager);

  private ParquetMetadata readFooter(Configuration conf, String path, ParquetReaderConfig readerConfig) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(path),
      readerConfig.addCountersToConf(conf)), readerConfig.toReadOptions())) {
      return reader.getFooter();
    }
  }

  /**
   * Helper class responsible for creating and managing DrillFileSystem.
   */
  protected abstract class AbstractDrillFileSystemManager {

    protected final OperatorContext operatorContext;

    protected AbstractDrillFileSystemManager(OperatorContext operatorContext) {
      this.operatorContext = operatorContext;
    }

    protected abstract DrillFileSystem get(Configuration config, String path) throws ExecutionSetupException;
  }
}
