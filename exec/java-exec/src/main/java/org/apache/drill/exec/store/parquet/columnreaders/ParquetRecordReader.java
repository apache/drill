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

import org.apache.drill.exec.store.CommonParquetRecordReader;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.store.parquet.columnreaders.batchsizing.RecordBatchSizerManager;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchStatsContext;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetRecordReader extends CommonParquetRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  /** Set when caller wants to read all the rows contained within the Parquet file */
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

  /** Parquet Schema */
  ParquetSchema schema;

  private FileSystem fileSystem;
  private final long numRecordsToRead; // number of records to read

  Path hadoopPath;

  private final CodecFactory codecFactory;
  int rowGroupIndex;

  ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus;

  /** Container object for holding Parquet columnar readers state */
  ReadState readState;
  /** Responsible for managing record batch size constraints */
  RecordBatchSizerManager batchSizerMgr;

  public boolean useAsyncColReader;
  public boolean useAsyncPageReader;
  public boolean useBufferedReader;
  public int bufferedReadSize;
  public boolean useFadvise;
  public boolean enforceTotalSize;
  public long readQueueSize;
  public boolean useBulkReader;

  @SuppressWarnings("unused")
  private Path name;

  private BatchReader batchReader;

  public ParquetRecordReader(FragmentContext fragmentContext,
      Path path,
      int rowGroupIndex,
      long numRecordsToRead,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) {
    this(fragmentContext, numRecordsToRead, path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      Path path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) {
    this(fragmentContext, footer.getBlocks().get(rowGroupIndex).getRowCount(), path, rowGroupIndex, fs, codecFactory,
        footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(
      FragmentContext fragmentContext,
      long numRecordsToRead,
      Path path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) {
    super(footer, fragmentContext);

    this.name = path;
    this.hadoopPath = path;
    this.fileSystem = fs;
    this.codecFactory = codecFactory;
    this.rowGroupIndex = rowGroupIndex;
    this.dateCorruptionStatus = dateCorruptionStatus;
    this.numRecordsToRead = initNumRecordsToRead(numRecordsToRead, rowGroupIndex, footer);
    this.useAsyncColReader = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_COLUMNREADER_ASYNC).bool_val;
    this.useAsyncPageReader = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val;
    this.useBufferedReader = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ).bool_val;
    this.bufferedReadSize = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE).num_val.intValue();
    this.useFadvise = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_FADVISE).bool_val;
    this.readQueueSize = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE).num_val;
    this.enforceTotalSize = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE).bool_val;
    this.useBulkReader = fragmentContext.getOptions().getOption(ExecConstants.PARQUET_FLAT_READER_BULK).bool_val;

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

  public RecordBatchSizerManager getBatchSizesMgr() {
    return batchSizerMgr;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public FragmentContext getFragmentContext() {
    return fragmentContext;
  }

  /**
   * @return true if Parquet reader Bulk processing is enabled; false otherwise
   */
  public boolean useBulkReader() {
    return useBulkReader;
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
    batchSizerMgr = new RecordBatchSizerManager(fragmentContext.getOptions(), schema, numRecordsToRead, new RecordBatchStatsContext(fragmentContext, operatorContext));

    logger.debug("Reading row group({}) with {} records in file {}.", rowGroupIndex, footer.getBlocks().get(rowGroupIndex).getRowCount(),
        hadoopPath.toUri().getPath());

    try {
      schema.buildSchema();
      batchSizerMgr.setup();
      readState = new ReadState(schema, batchSizerMgr, parquetReaderStats, numRecordsToRead, useAsyncColReader);
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
    batchSizerMgr.allocate(vectorMap);
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

    // NOTE - We check whether the parquet reader data structure is not null before calling close();
    //        this is because close() can be invoked before setup() has executed (probably because of
    //        spurious failures).

    if (readState != null) {
      readState.close();
      readState = null;
    }

    if (batchSizerMgr != null) {
      batchSizerMgr.close();
      batchSizerMgr = null;
    }

    codecFactory.release();

    closeStats(logger, hadoopPath);
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return DEFAULT_COLS_TO_READ;
  }

  private int initNumRecordsToRead(long numRecordsToRead, int rowGroupIndex, ParquetMetadata footer) {
    if ( numRecordsToRead == 0 ) { return 0; } // runtime pruning sometimes prunes everything, needs one empty RG for the schema
    int numRowsInRowgroup = (int) footer.getBlocks().get(rowGroupIndex).getRowCount();
    // Callers can pass -1 if they want to read all rows.
    if (numRecordsToRead == NUM_RECORDS_TO_READ_NOT_SPECIFIED) {
      return numRowsInRowgroup;
    }
    assert (numRecordsToRead > 0);
    return (int) Math.min(numRecordsToRead, numRowsInRowgroup);
  }

  @Override
  public String toString() {
    return "ParquetRecordReader[File=" + hadoopPath.toUri()
        + ", Row group index=" + rowGroupIndex
        + ", Records in row group=" + footer.getBlocks().get(rowGroupIndex).getRowCount()
        + ", Total records read=" + (readState != null ? readState.recordsRead() : -1)
        + ", Metadata" + footer
        + "]";
  }
}
