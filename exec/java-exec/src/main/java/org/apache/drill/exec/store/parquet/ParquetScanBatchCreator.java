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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Functions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.store.parquet2.DrillParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


public class ParquetScanBatchCreator implements BatchCreator<ParquetRowGroupScan>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetScanBatchCreator.class);

  private static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  private static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  private static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  @Override
  public ScanBatch getBatch(FragmentContext context, ParquetRowGroupScan rowGroupScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    OperatorContext oContext = context.newOperatorContext(rowGroupScan);

    final ImplicitColumnExplorer columnExplorer = new ImplicitColumnExplorer(context, rowGroupScan.getColumns());

    if (!columnExplorer.isStarQuery()) {
      rowGroupScan = new ParquetRowGroupScan(rowGroupScan.getUserName(), rowGroupScan.getStorageEngine(),
          rowGroupScan.getRowGroupReadEntries(), columnExplorer.getTableColumns(), rowGroupScan.getSelectionRoot());
      rowGroupScan.setOperatorId(rowGroupScan.getOperatorId());
    }

    DrillFileSystem fs;
    try {
      fs = oContext.newFileSystem(rowGroupScan.getStorageEngine().getFsConf());
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create DrillFileSystem: %s", e.getMessage()), e);
    }
    Configuration conf = new Configuration(fs.getConf());
    conf.setBoolean(ENABLE_BYTES_READ_COUNTER, false);
    conf.setBoolean(ENABLE_BYTES_TOTAL_COUNTER, false);
    conf.setBoolean(ENABLE_TIME_READ_COUNTER, false);

    // keep footers in a map to avoid re-reading them
    Map<String, ParquetMetadata> footers = Maps.newHashMap();
    List<RecordReader> readers = Lists.newArrayList();
    List<Map<String, String>> implicitColumns = Lists.newArrayList();
    Map<String, String> mapWithMaxColumns = Maps.newLinkedHashMap();
    for(RowGroupReadEntry e : rowGroupScan.getRowGroupReadEntries()){
      /*
      Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      try {
        Stopwatch timer = Stopwatch.createUnstarted();
        if (!footers.containsKey(e.getPath())){
          timer.start();
          ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(e.getPath()));
          long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
          logger.trace("ParquetTrace,Read Footer,{},{},{},{},{},{},{}", "", e.getPath(), "", 0, 0, 0, timeToRead);
          footers.put(e.getPath(), footer );
        }
        if (!context.getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val && !isComplex(footers.get(e.getPath()))) {
          readers.add(
              new ParquetRecordReader(
                  context, e.getPath(), e.getRowGroupIndex(), fs,
                  CodecFactory.createDirectCodecFactory(
                  fs.getConf(),
                  new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
                  footers.get(e.getPath()),
                  rowGroupScan.getColumns()
              )
          );
        } else {
          ParquetMetadata footer = footers.get(e.getPath());
          readers.add(new DrillParquetReader(context, footer, e, columnExplorer.getTableColumns(), fs));
        }

        Map<String, String> implicitValues = columnExplorer.populateImplicitColumns(e, rowGroupScan.getSelectionRoot());
        implicitColumns.add(implicitValues);
        if (implicitValues.size() > mapWithMaxColumns.size()) {
          mapWithMaxColumns = implicitValues;
        }

      } catch (IOException e1) {
        throw new ExecutionSetupException(e1);
      }
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant((String) null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(rowGroupScan, context, oContext, readers.iterator(), implicitColumns);
  }

  private static boolean isComplex(ParquetMetadata footer) {
    MessageType schema = footer.getFileMetaData().getSchema();

    for (Type type : schema.getFields()) {
      if (!type.isPrimitive()) {
        return true;
      }
    }
    for (ColumnDescriptor col : schema.getColumns()) {
      if (col.getMaxRepetitionLevel() > 0) {
        return true;
      }
    }
    return false;
  }

}
