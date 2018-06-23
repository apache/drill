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
package org.apache.drill.exec.store.parquet.columnreaders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Base strategy for reading a batch of Parquet records.
 */
public abstract class BatchReader {

  protected final ReadState readState;

  public BatchReader(ReadState readState) {
    this.readState = readState;
  }

  public int readBatch() throws Exception {
    ColumnReader<?> firstColumnStatus = readState.getFirstColumnReader();
    long recordsToRead = Math.min(getReadCount(firstColumnStatus), readState.getRecordsToRead());
    int readCount = readRecords(firstColumnStatus, recordsToRead);
    readState.fillNullVectors(readCount);
    return readCount;
  }

  protected abstract long getReadCount(ColumnReader<?> firstColumnStatus);

  protected abstract int readRecords(ColumnReader<?> firstColumnStatus, long recordsToRead) throws Exception;

  protected void readAllFixedFields(long recordsToRead) throws Exception {
    Stopwatch timer = Stopwatch.createStarted();
    if(readState.useAsyncColReader()){
      readAllFixedFieldsParallel(recordsToRead);
    } else {
      readAllFixedFieldsSerial(recordsToRead);
    }
    readState.parquetReaderStats().timeFixedColumnRead.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
  }

  protected void readAllFixedFieldsSerial(long recordsToRead) throws IOException {
    for (ColumnReader<?> crs : readState.getColumnReaders()) {
      crs.processPages(recordsToRead);
    }
  }

  protected void readAllFixedFieldsParallel(long recordsToRead) throws Exception {
    ArrayList<Future<Long>> futures = Lists.newArrayList();
    for (ColumnReader<?> crs : readState.getColumnReaders()) {
      Future<Long> f = crs.processPagesAsync(recordsToRead);
      if (f != null) {
        futures.add(f);
      }
    }
    Exception exception = null;
    for(Future<Long> f: futures){
      if (exception != null) {
        f.cancel(true);
      } else {
        try {
          f.get();
        } catch (Exception e) {
          f.cancel(true);
          exception = e;
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * Strategy for reading mock records. Mock records appear to occur in the case
   * in which the query has SELECT a, b, but the Parquet file has only c, d.
   * A mock scan reads dummy columns for all records to ensure that the batch
   * contains a record for each Parquet record, but with no data per record.
   * (This explanation is reverse-engineered from the code and may be wrong.
   * Caveat emptor!)
   */

  public static class MockBatchReader extends BatchReader {

    public MockBatchReader(ReadState readState) {
      super(readState);
    }

    @Override
    protected long getReadCount(ColumnReader<?> firstColumnStatus) {
      if (readState.recordsRead() == readState.schema().getGroupRecordCount()) {
        return 0;
      }
      return Math.min(ParquetRecordReader.DEFAULT_RECORDS_TO_READ_IF_VARIABLE_WIDTH,
                      readState.schema().getGroupRecordCount() - readState.recordsRead());
    }

    @Override
    protected int readRecords(ColumnReader<?> firstColumnStatus, long recordsToRead) {
      readState.updateCounts((int) recordsToRead);
      return (int) recordsToRead;
    }
  }

  /**
   * Strategy for reading a record batch when all columns are
   * fixed-width.
   */

  public static class FixedWidthReader extends BatchReader {

    public FixedWidthReader(ReadState readState) {
      super(readState);
    }

    @Override
    protected long getReadCount(ColumnReader<?> firstColumnStatus) {
      return Math.min(readState.schema().getRecordsPerBatch(),
                      firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
    }

    @Override
    protected int readRecords(ColumnReader<?> firstColumnStatus, long recordsToRead) throws Exception {
      readAllFixedFields(recordsToRead);
      return firstColumnStatus.getRecordsReadInCurrentPass();
    }
  }

  /**
   * Strategy for reading a record batch when at last one column is
   * variable width.
   */

  public static class VariableWidthReader extends BatchReader {

    public VariableWidthReader(ReadState readState) {
      super(readState);
    }

    @Override
    protected long getReadCount(ColumnReader<?> firstColumnStatus) {
      return ParquetRecordReader.DEFAULT_RECORDS_TO_READ_IF_VARIABLE_WIDTH;
    }

    @Override
    protected int readRecords(ColumnReader<?> firstColumnStatus, long recordsToRead) throws Exception {
      long fixedRecordsToRead = readState.varLengthReader().readFields(recordsToRead);
      readAllFixedFields(fixedRecordsToRead);
      return firstColumnStatus.getRecordsReadInCurrentPass();
    }
  }
}
