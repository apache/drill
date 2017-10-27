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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

public class ParquetRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final int NUMBER_OF_VECTORS = 1;
  private static final long DEFAULT_BATCH_LENGTH = 256 * 1024 * NUMBER_OF_VECTORS; // 256kb
  private static final long DEFAULT_BATCH_LENGTH_IN_BITS = DEFAULT_BATCH_LENGTH * 8; // 256kb
  static final char DEFAULT_RECORDS_TO_READ_IF_VARIABLE_WIDTH = 32*1024; // 32K
  static final int DEFAULT_RECORDS_TO_READ_IF_FIXED_WIDTH = 64*1024 - 1; // 64K - 1, max SV2 can address
  static final int NUM_RECORDS_TO_READ_NOT_SPECIFIED = -1;

  // When no column is required by the downstream operator, ask SCAN to return a DEFAULT column. If such column does not exist,
  // it will return as a nullable-int column. If that column happens to exist, return that column.
  protected static final List<SchemaPath> DEFAULT_COLS_TO_READ = ImmutableList.of(SchemaPath.getSimplePath("_DEFAULT_COL_TO_READ_"));

  // TODO - should probably find a smarter way to set this, currently 1 megabyte
  public static final int PARQUET_PAGE_MAX_SIZE = 1024 * 1024 * 1;

  // used for clearing the last n bits of a byte
  public static final byte[] endBitMasks = {-2, -4, -8, -16, -32, -64, -128};
  // used for clearing the first n bits of a byte
  public static final byte[] startBitMasks = {127, 63, 31, 15, 7, 3, 1};

  private OperatorContext operatorContext;

  private FileSystem fileSystem;
  private final long batchSize;
  private long numRecordsToRead; // number of records to read

  Path hadoopPath;
  private ParquetMetadata footer;

  private final CodecFactory codecFactory;
  int rowGroupIndex;
  private final FragmentContext fragmentContext;
  ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus;

  ParquetSchema schema;
  ReadState readState;

  public boolean useAsyncColReader;
  public boolean useAsyncPageReader;
  public boolean useBufferedReader;
  public int bufferedReadSize;
  public boolean useFadvise;
  public boolean enforceTotalSize;
  public long readQueueSize;

  @SuppressWarnings("unused")
  private String name;

  public ParquetReaderStats parquetReaderStats = new ParquetReaderStats();
  private BatchReader batchReader;

  public enum Metric implements MetricDef {
    NUM_DICT_PAGE_LOADS,         // Number of dictionary pages read
    NUM_DATA_PAGE_lOADS,         // Number of data pages read
    NUM_DATA_PAGES_DECODED,      // Number of data pages decoded
    NUM_DICT_PAGES_DECOMPRESSED, // Number of dictionary pages decompressed
    NUM_DATA_PAGES_DECOMPRESSED, // Number of data pages decompressed
    TOTAL_DICT_PAGE_READ_BYTES,  // Total bytes read from disk for dictionary pages
    TOTAL_DATA_PAGE_READ_BYTES,  // Total bytes read from disk for data pages
    TOTAL_DICT_DECOMPRESSED_BYTES, // Total bytes decompressed for dictionary pages (same as compressed bytes on disk)
    TOTAL_DATA_DECOMPRESSED_BYTES, // Total bytes decompressed for data pages (same as compressed bytes on disk)
    TIME_DICT_PAGE_LOADS,          // Time in nanos in reading dictionary pages from disk
    TIME_DATA_PAGE_LOADS,          // Time in nanos in reading data pages from disk
    TIME_DATA_PAGE_DECODE,         // Time in nanos in decoding data pages
    TIME_DICT_PAGE_DECODE,         // Time in nanos in decoding dictionary pages
    TIME_DICT_PAGES_DECOMPRESSED,  // Time in nanos in decompressing dictionary pages
    TIME_DATA_PAGES_DECOMPRESSED,  // Time in nanos in decompressing data pages
    TIME_DISK_SCAN_WAIT,           // Time in nanos spent in waiting for an async disk read to complete
    TIME_DISK_SCAN,                // Time in nanos spent in reading data from disk.
    TIME_FIXEDCOLUMN_READ,         // Time in nanos spent in converting fixed width data to value vectors
    TIME_VARCOLUMN_READ,           // Time in nanos spent in converting varwidth data to value vectors
    TIME_PROCESS;                  // Time in nanos spent in processing

    @Override public int metricId() {
      return ordinal();
    }
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      long numRecordsToRead,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) throws ExecutionSetupException {
    this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, numRecordsToRead,
         path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus)
      throws ExecutionSetupException {
      this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, footer.getBlocks().get(rowGroupIndex).getRowCount(),
           path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(
      FragmentContext fragmentContext,
      long batchSize,
      long numRecordsToRead,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) throws ExecutionSetupException {
    this.name = path;
    this.hadoopPath = new Path(path);
    this.fileSystem = fs;
    this.codecFactory = codecFactory;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;
    this.footer = footer;
    this.dateCorruptionStatus = dateCorruptionStatus;
    this.fragmentContext = fragmentContext;
    this.numRecordsToRead = numRecordsToRead;
    useAsyncColReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_COLUMNREADER_ASYNC).bool_val;
    useAsyncPageReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val;
    useBufferedReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ).bool_val;
    bufferedReadSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE).num_val.intValue();
    useFadvise =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_FADVISE).bool_val;
    readQueueSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE).num_val;
    enforceTotalSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE).bool_val;

    setColumns(columns);
  }

  /**
   * Flag indicating if the old non-standard data format appears
   * in this file, see DRILL-4203.
   *
   * @return true if the dates are corrupted and need to be corrected
   */
  public ParquetReaderUtility.DateCorruptionStatus getDateCorruptionStatus() {
    return dateCorruptionStatus;
  }

  public CodecFactory getCodecFactory() {
    return codecFactory;
  }

  public Path getHadoopPath() {
    return hadoopPath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  public int getBitWidthAllFixedFields() {
    return schema.getBitWidthAllFixedFields();
  }

  public long getBatchSize() {
    return batchSize;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public FragmentContext getFragmentContext() {
    return fragmentContext;
  }

  /**
   * Prepare the Parquet reader. First determine the set of columns to read (the schema
   * for this read.) Then, create a state object to track the read across calls to
   * the reader <tt>next()</tt> method. Finally, create one of three readers to
   * read batches depending on whether this scan is for only fixed-width fields,
   * contains at least one variable-width field, or is a "mock" scan consisting
   * only of null fields (fields in the SELECT clause but not in the Parquet file.)
   */

  @Override
  public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = operatorContext;
    schema = new ParquetSchema(fragmentContext.getOptions(), rowGroupIndex, footer, isStarQuery() ? null : getColumns());

    logger.debug("Reading row group({}) with {} records in file {}.", rowGroupIndex, footer.getBlocks().get(rowGroupIndex).getRowCount(),
        hadoopPath.toUri().getPath());

    try {
      schema.buildSchema(batchSize);
      readState = new ReadState(schema, parquetReaderStats, numRecordsToRead, useAsyncColReader);
      readState.buildReader(this, output);
    } catch (Exception e) {
      throw handleException("Failure in setting up reader", e);
    }

    ColumnReader<?> firstColumnStatus = readState.getFirstColumnReader();
    if (firstColumnStatus == null) {
      batchReader = new BatchReader.MockBatchReader(readState);
    } else if (schema.allFieldsFixedLength()) {
      batchReader = new BatchReader.FixedWidthReader(readState);
    } else {
      batchReader = new BatchReader.VariableWidthReader(readState);
    }
  }

  protected DrillRuntimeException handleException(String s, Exception e) {
    String message = "Error in parquet record reader.\nMessage: " + s +
      "\nParquet Metadata: " + footer;
    return new DrillRuntimeException(message, e);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      int recordsPerBatch = schema.getRecordsPerBatch();
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, recordsPerBatch, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  /**
   * Read the next record batch from the file using the reader and read state
   * created previously.
   */

  @Override
  public int next() {
    readState.resetBatch();
    Stopwatch timer = Stopwatch.createStarted();
    try {
      return batchReader.readBatch();
    } catch (Exception e) {
      throw handleException("\nHadoop path: " + hadoopPath.toUri().getPath() +
        "\nTotal records read: " + readState.recordsRead() +
        "\nRow group index: " + rowGroupIndex +
        "\nRecords in row group: " + footer.getBlocks().get(rowGroupIndex).getRowCount(), e);
    } finally {
      parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
    }
  }

  @Override
  public void close() {
    long recordsRead = (readState == null) ? 0 : readState.recordsRead();
    logger.debug("Read {} records out of row group({}) in file '{}'",
        recordsRead, rowGroupIndex,
        hadoopPath.toUri().getPath());
    // enable this for debugging when it is know that a whole file will be read
    // limit kills upstream operators once it has enough records, so this assert will fail
//    assert totalRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount();
    if (readState != null) {
      readState.close();
      readState = null;
    }

    codecFactory.release();

    if (parquetReaderStats != null) {
      updateStats();
      parquetReaderStats.logStats(logger, hadoopPath);
      parquetReaderStats = null;
    }
  }

  private void updateStats() {
    parquetReaderStats.update(operatorContext.getStats());
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return DEFAULT_COLS_TO_READ;
  }
}
