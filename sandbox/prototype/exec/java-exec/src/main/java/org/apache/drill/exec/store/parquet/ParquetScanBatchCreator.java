/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.mock.MockScanBatchCreator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

public class ParquetScanBatchCreator implements BatchCreator<ParquetRowGroupScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetScanBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, ParquetRowGroupScan rowGroupScan, List<RecordBatch> children) throws ExecutionSetupException {
    Stopwatch watch = new Stopwatch();
    watch.start();
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = Lists.newArrayList();
    for(ParquetRowGroupScan.RowGroupReadEntry e : rowGroupScan.getRowGroupReadEntries()){
      /*
      Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      try {
        readers.add(
            new ParquetRecordReader(
                context, e.getPath(), e.getRowGroupIndex(), rowGroupScan.getStorageEngine().getFileSystem(),
                rowGroupScan.getStorageEngine().getCodecFactoryExposer(),
                ParquetFileReader.readFooter( rowGroupScan.getStorageEngine().getFileSystem().getConf(), new Path(e.getPath())),
                rowGroupScan.getRef()
            )
        );
      } catch (IOException e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    logger.debug("total time in ScanBatchCreator.getBatch: {} ms", watch.elapsed(TimeUnit.MILLISECONDS));
    return new ScanBatch(context, readers.iterator());
  }
}
