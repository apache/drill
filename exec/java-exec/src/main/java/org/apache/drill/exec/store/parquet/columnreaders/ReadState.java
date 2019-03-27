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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.store.parquet.columnreaders.batchsizing.RecordBatchSizerManager;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

/**
 * Internal state for reading from a Parquet file. Tracks information
 * required from one call of <tt>next()</tt> to the next.
 * <p>
 * At present, this is a bit of a muddle as it holds all read state.
 * As such, this is a snapshot of a refactoring effort. Subsequent passes
 * will move state into specific readers where possible.
 */

public class ReadState {
  /** The Parquet Schema */
  private final ParquetSchema schema;
  /** Responsible for managing record batch size constraints */
  private final RecordBatchSizerManager batchSizerMgr;
  private final ParquetReaderStats parquetReaderStats;
  private VarLenBinaryReader varLengthReader;
  /**
   * For columns not found in the file, we need to return a schema element with the correct number of values
   * at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
   * that need only have their value count set at the end of each call to next(), as the values default to null.
   */
  private List<NullableIntVector> nullFilledVectors;
  private List<ColumnReader<?>> fixedLenColumnReaders = new ArrayList<>();
  private final long totalNumRecordsToRead; // number of records to read

  // counter for the values that have been read in this pass (a single call to the next() method)
  private int valuesReadInCurrentBatch;

  /**
   * Keeps track of the number of records read thus far.
   * <p>
   * Also keeps track of the number of records returned in the case where only columns outside of the file were selected.
   * No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
   * records specified in the row group metadata.
   */
  private long totalRecordsRead;
  private boolean useAsyncColReader;

  public ReadState(ParquetSchema schema,
    RecordBatchSizerManager batchSizerMgr, ParquetReaderStats parquetReaderStats, long numRecordsToRead,
    boolean useAsyncColReader) {

    this.schema = schema;
    this.batchSizerMgr = batchSizerMgr;
    this.parquetReaderStats = parquetReaderStats;
    this.useAsyncColReader = useAsyncColReader;
    if (! schema.isStarQuery()) {
      nullFilledVectors = new ArrayList<>();
    }

    // In the case where runtime pruning prunes out all the rowgroups, then just a single rowgroup
    // with zero rows is read (in order to get the schema, no need for the rows)
    if ( numRecordsToRead == 0 ) {
      this.totalNumRecordsToRead = 0;
      return;
    }

    // Because of JIRA DRILL-6528, the Parquet reader is sometimes getting the wrong
    // number of rows to read. For now, returning all a file data (till
    // downstream operator stop consuming).
    numRecordsToRead = -1;

    // Callers can pass -1 if they want to read all rows.
    if (numRecordsToRead == ParquetRecordReader.NUM_RECORDS_TO_READ_NOT_SPECIFIED) {
      this.totalNumRecordsToRead = schema.getGroupRecordCount();
    } else {
      assert (numRecordsToRead >= 0);
      this.totalNumRecordsToRead = Math.min(numRecordsToRead, schema.getGroupRecordCount());
    }
  }

  /**
   * Create the readers needed to read columns: fixed-length or variable length.
   *
   * @param reader
   * @param output
   * @throws Exception
   */

  @SuppressWarnings("unchecked")
  public void buildReader(ParquetRecordReader reader, OutputMutator output) throws Exception {
    final ArrayList<VarLengthColumn<? extends ValueVector>> varLengthColumns = new ArrayList<>();
    // initialize all of the column read status objects
    BlockMetaData rowGroupMetadata = schema.getRowGroupMetadata();
    Map<String, Integer> columnChunkMetadataPositionsInList = schema.buildChunkMap(rowGroupMetadata);
    for (ParquetColumnMetadata columnMetadata : schema.getColumnMetadata()) {
      ColumnDescriptor column = columnMetadata.column;
      columnMetadata.columnChunkMetaData = rowGroupMetadata.getColumns().get(
                      columnChunkMetadataPositionsInList.get(Arrays.toString(column.getPath())));
      columnMetadata.buildVector(output);
      if (! columnMetadata.isFixedLength( )) {
        // create a reader and add it to the appropriate list
        varLengthColumns.add(columnMetadata.makeVariableWidthReader(reader));
      } else if (columnMetadata.isRepeated()) {
        varLengthColumns.add(columnMetadata.makeRepeatedFixedWidthReader(reader));
      }
      else {
        fixedLenColumnReaders.add(columnMetadata.makeFixedWidthReader(reader));
      }
    }
    varLengthReader = new VarLenBinaryReader(reader, varLengthColumns);
    if (! schema.isStarQuery()) {
      schema.createNonExistentColumns(output, nullFilledVectors);
    }
  }

  /**
   * Several readers use the first column reader to get information about the whole
   * record or group (such as row count.)
   *
   * @return the reader for the first column
   */

  public ColumnReader<?> getFirstColumnReader() {
    if (fixedLenColumnReaders.size() > 0) {
      return fixedLenColumnReaders.get(0);
    }
    else if (varLengthReader.columns.size() > 0) {
      return varLengthReader.columns.get(0);
    } else {
      return null;
    }
  }

  public void resetBatch() {
    for (final ColumnReader<?> column : fixedLenColumnReaders) {
      column.valuesReadInCurrentPass = 0;
    }
    for (final VarLengthColumn<?> r : varLengthReader.columns) {
      r.valuesReadInCurrentPass = 0;
    }
    setValuesReadInCurrentPass(0);
  }

  public ParquetSchema schema() { return schema; }
  public RecordBatchSizerManager batchSizerMgr() { return batchSizerMgr; }
  public List<ColumnReader<?>> getFixedLenColumnReaders() { return fixedLenColumnReaders; }
  public long recordsRead() { return totalRecordsRead; }
  public VarLenBinaryReader varLengthReader() { return varLengthReader; }
  public long getTotalRecordsToRead() { return totalNumRecordsToRead; }
  public boolean useAsyncColReader() { return useAsyncColReader; }
  public ParquetReaderStats parquetReaderStats() { return parquetReaderStats; }

  /**
   * @return values read within the latest batch
   */
  public int getValuesReadInCurrentPass() {
    return valuesReadInCurrentBatch;
  }

  /**
   * @return remaining values to read
   */
  public int getRemainingValuesToRead() {
    assert totalNumRecordsToRead >= totalRecordsRead;
    return (int) (totalNumRecordsToRead - totalRecordsRead);
  }

  /**
   * @param valuesReadInCurrentBatch the valuesReadInCurrentBatch to set
   */
  public void setValuesReadInCurrentPass(int valuesReadInCurrentBatch) {
    this.valuesReadInCurrentBatch = valuesReadInCurrentBatch;
  }


  /**
   * When the SELECT clause references columns that do not exist in the Parquet
   * file, we don't issue an error; instead we simply make up a column and
   * fill it with nulls. This method does the work of null-filling the made-up
   * vectors.
   *
   * @param readCount the number of rows read in the present record batch,
   * which is the number of null column values to create
   */

  public void fillNullVectors(int readCount) {

    // if we have requested columns that were not found in the file fill their vectors with null
    // (by simply setting the value counts inside of them, as they start null filled)

    if (nullFilledVectors != null) {
      for (final ValueVector vv : nullFilledVectors ) {
        vv.getMutator().setValueCount(readCount);
      }
    }
  }

  public void updateCounts(int readCount) {
    totalRecordsRead += readCount;
  }

  public void close() {
    if (fixedLenColumnReaders != null) {
      for (final ColumnReader<?> column : fixedLenColumnReaders) {
        column.clear();
      }
      fixedLenColumnReaders.clear();
      fixedLenColumnReaders = null;
    }
    if (varLengthReader != null) {
      for (final VarLengthColumn<? extends ValueVector> r : varLengthReader.columns) {
        r.clear();
      }
      varLengthReader.columns.clear();
      varLengthReader = null;
    }
  }
}