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
package org.apache.drill.exec.store.parquet.columnreaders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
import parquet.format.FileMetaData;
import parquet.format.SchemaElement;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.PrimitiveType;

public class ParquetRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final int NUMBER_OF_VECTORS = 1;
  private static final long DEFAULT_BATCH_LENGTH = 256 * 1024 * NUMBER_OF_VECTORS; // 256kb
  private static final long DEFAULT_BATCH_LENGTH_IN_BITS = DEFAULT_BATCH_LENGTH * 8; // 256kb
  private static final char DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH = 32*1024;

  // TODO - should probably find a smarter way to set this, currently 1 megabyte
  private static final int VAR_LEN_FIELD_LENGTH = 1024 * 1024 * 1;
  public static final int PARQUET_PAGE_MAX_SIZE = 1024 * 1024 * 1;
  private static final String SEPERATOR = System.getProperty("file.separator");

  // used for clearing the last n bits of a byte
  public static final byte[] endBitMasks = {-2, -4, -8, -16, -32, -64, -128};
  // used for clearing the first n bits of a byte
  public static final byte[] startBitMasks = {127, 63, 31, 15, 7, 3, 1};

  private int bitWidthAllFixedFields;
  private boolean allFieldsFixedLength;
  private int recordsPerBatch;
//  private long totalRecords;
//  private long rowGroupOffset;

  private List<ColumnReader> columnStatuses;
  private FileSystem fileSystem;
  private long batchSize;
  Path hadoopPath;
  private VarLenBinaryReader varLengthReader;
  private ParquetMetadata footer;
  private OperatorContext operatorContext;
  // This is a parallel list to the columns list above, it is used to determine the subset of the project
  // pushdown columns that do not appear in this file
  private boolean[] columnsFound;
  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<NullableBitVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead;

  private final CodecFactoryExposer codecFactoryExposer;
  int rowGroupIndex;

  public ParquetRecordReader(FragmentContext fragmentContext, //
                             String path, //
                             int rowGroupIndex, //
                             FileSystem fs, //
                             CodecFactoryExposer codecFactoryExposer, //
                             ParquetMetadata footer, //
                             List<SchemaPath> columns) throws ExecutionSetupException {
    this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, path, rowGroupIndex, fs, codecFactoryExposer, footer,
        columns);
  }

  public ParquetRecordReader(FragmentContext fragmentContext, long batchSize,
                             String path, int rowGroupIndex, FileSystem fs,
                             CodecFactoryExposer codecFactoryExposer, ParquetMetadata footer,
                             List<SchemaPath> columns) throws ExecutionSetupException {
    this.hadoopPath = new Path(path);
    this.fileSystem = fs;
    this.codecFactoryExposer = codecFactoryExposer;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;
    this.footer = footer;
    setColumns(columns);
  }

  public CodecFactoryExposer getCodecFactoryExposer() {
    return codecFactoryExposer;
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
    return bitWidthAllFixedFields;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in pageDataByteArray of the type
   */
  public static int getTypeLengthInBits(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:   return 64;
      case INT32:   return 32;
      case BOOLEAN: return 1;
      case FLOAT:   return 32;
      case DOUBLE:  return 64;
      case INT96:   return 96;
      // binary and fixed length byte array
      default:
        throw new IllegalStateException("Length cannot be determined for type " + type);
    }
  }

  private boolean fieldSelected(MaterializedField field){
    // TODO - not sure if this is how we want to represent this
    // for now it makes the existing tests pass, simply selecting
    // all available data if no columns are provided
    if (isStarQuery()) {
      return true;
    }

    int i = 0;
    for (SchemaPath expr : getColumns()){
      if ( field.matches(expr)){
        columnsFound[i] = true;
        return true;
      }
      i++;
    }
    return false;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    if (!isStarQuery()) {
      columnsFound = new boolean[getColumns().size()];
      nullFilledVectors = new ArrayList();
    }
    columnStatuses = new ArrayList<>();
//    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();
    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;
    int columnsToScan = 0;
    mockRecordsRead = 0;

    MaterializedField field;
//    ParquetMetadataConverter metaConverter = new ParquetMetadataConverter();
    FileMetaData fileMetaData;

    // TODO - figure out how to deal with this better once we add nested reading, note also look where this map is used below
    // store a map from column name to converted types if they are non-null
    HashMap<String, SchemaElement> schemaElements = new HashMap<>();
    fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    for (SchemaElement se : fileMetaData.getSchema()) {
      schemaElements.put(se.getName(), se);
    }

    // loop to add up the length of the fixed width columns and build the schema
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      logger.debug("name: " + fileMetaData.getSchema().get(i).name);
      SchemaElement se = schemaElements.get(column.getPath()[0]);
      MajorType mt = ParquetToDrillTypeConverter.toMajorType(column.getType(), se.getType_length(), getDataMode(column), se);
      field = MaterializedField.create(toFieldName(column.getPath()),mt);
      if ( ! fieldSelected(field)){
        continue;
      }
      columnsToScan++;
      // sum the lengths of all of the fixed length fields
      if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
        if (column.getMaxRepetitionLevel() > 0) {
          allFieldsFixedLength = false;
        }
        // There is not support for the fixed binary type yet in parquet, leaving a task here as a reminder
        // TODO - implement this when the feature is added upstream
          if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY){
              bitWidthAllFixedFields += se.getType_length() * 8;
          } else {
            bitWidthAllFixedFields += getTypeLengthInBits(column.getType());
          }
      } else {
        allFieldsFixedLength = false;
      }
    }
//    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

    if (columnsToScan != 0  && allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(Math.min(batchSize / bitWidthAllFixedFields,
          footer.getBlocks().get(0).getColumns().get(0).getValueCount()), 65535);
    }
    else {
      recordsPerBatch = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
    }

    try {
      ValueVector v;
      SchemaElement schemaElement;
      ArrayList<VarLengthColumn> varLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength;
      for (int i = 0; i < columns.size(); ++i) {
        column = columns.get(i);
        columnChunkMetaData = footer.getBlocks().get(rowGroupIndex).getColumns().get(i);
        schemaElement = schemaElements.get(column.getPath()[0]);
        MajorType type = ParquetToDrillTypeConverter.toMajorType(column.getType(), schemaElement.getType_length(), getDataMode(column), schemaElement);
        field = MaterializedField.create(toFieldName(column.getPath()), type);
        // the field was not requested to be read
        if ( ! fieldSelected(field)) continue;

        fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
        v = output.addField(field, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          if (column.getMaxRepetitionLevel() > 0) {
            ColumnReader dataReader = ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength, column, columnChunkMetaData, recordsPerBatch,
                ((RepeatedFixedWidthVector) v).getMutator().getDataVector(), schemaElement);
            varLengthColumns.add(new FixedWidthRepeatedReader(this, dataReader,
                getTypeLengthInBits(column.getType()), -1, column, columnChunkMetaData, false, v, schemaElement));
          }
          else {
            columnStatuses.add(ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength, column, columnChunkMetaData, recordsPerBatch, v,
                schemaElement));
          }
        } else {
          // create a reader and add it to the appropriate list
          varLengthColumns.add(ColumnReaderFactory.getReader(this, -1, column, columnChunkMetaData, false, v, schemaElement));
        }
      }
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns);

      if (!isStarQuery()) {
        List<SchemaPath> projectedColumns = Lists.newArrayList(getColumns());
        SchemaPath col;
        for (int i = 0; i < columnsFound.length; i++) {
          col = projectedColumns.get(i);
          assert col!=null;
          if ( ! columnsFound[i] && !col.equals(STAR_COLUMN)) {
            nullFilledVectors.add((NullableBitVector)output.addField(MaterializedField.create(col,
                    Types.optional(TypeProtos.MinorType.BIT)),
                (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(TypeProtos.MinorType.BIT, DataMode.OPTIONAL)));

          }
        }
      }
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  private SchemaPath toFieldName(String[] paths) {
    return SchemaPath.getCompoundPath(paths);
  }

  private TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxRepetitionLevel() > 0 ) {
      return DataMode.REPEATED;
    } else if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (ColumnReader column : columnStatuses) {
      column.valuesReadInCurrentPass = 0;
    }
    for (VarLengthColumn r : varLengthReader.columns){
      r.valuesReadInCurrentPass = 0;
    }
  }

 public void readAllFixedFields(long recordsToRead) throws IOException {

   for (ColumnReader crs : columnStatuses){
     crs.processPages(recordsToRead);
   }
 }

  @Override
  public int next() {
    resetBatch();
    long recordsToRead = 0;
    try {
      ColumnReader firstColumnStatus;
      if (columnStatuses.size() > 0){
        firstColumnStatus = columnStatuses.iterator().next();
      }
      else{
        if (varLengthReader.columns.size() > 0){
          firstColumnStatus = varLengthReader.columns.iterator().next();
        }
        else{
          firstColumnStatus = null;
        }
      }
      // No columns found in the file were selected, simply return a full batch of null records for each column requested
      if (firstColumnStatus == null) {
        if (mockRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount()) {
          return 0;
        }
        recordsToRead = Math.min(DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH, footer.getBlocks().get(rowGroupIndex).getRowCount() - mockRecordsRead);
        for (ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount( (int) recordsToRead);
        }
        mockRecordsRead += recordsToRead;
        return (int) recordsToRead;
      }

      if (allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
      } else {
        recordsToRead = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;

      }

      if (allFieldsFixedLength) {
        readAllFixedFields(recordsToRead);
      } else { // variable length columns
        long fixedRecordsToRead = varLengthReader.readFields(recordsToRead, firstColumnStatus);
        readAllFixedFields(fixedRecordsToRead);
      }

      // if we have requested columns that were not found in the file fill their vectors with null
      // (by simply setting the value counts inside of them, as they start null filled)
      if (nullFilledVectors != null) {
        for (ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount(firstColumnStatus.getRecordsReadInCurrentPass());
        }
      }

      return firstColumnStatus.getRecordsReadInCurrentPass();
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    for (ColumnReader column : columnStatuses) {
      column.clear();
    }
    columnStatuses.clear();

    for (VarLengthColumn r : varLengthReader.columns){
      r.clear();
    }
    varLengthReader.columns.clear();
  }
}
