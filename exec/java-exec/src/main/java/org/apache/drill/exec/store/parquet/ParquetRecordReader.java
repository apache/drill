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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.PrimitiveType;

import com.google.common.base.Joiner;

class ParquetRecordReader implements RecordReader {
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
  private long totalRecords;
  private long rowGroupOffset;

  private List<ColumnReader> columnStatuses;
  FileSystem fileSystem;
  BufferAllocator allocator;
  private long batchSize;
  Path hadoopPath;
  private VarLenBinaryReader varLengthReader;
  private ParquetMetadata footer;
  private List<SchemaPath> columns;

  public CodecFactoryExposer getCodecFactoryExposer() {
    return codecFactoryExposer;
  }

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
    this.allocator = fragmentContext.getAllocator();
    hadoopPath = new Path(path);
    fileSystem = fs;
    this.codecFactoryExposer = codecFactoryExposer;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;
    this.footer = footer;
    this.columns = columns;
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
    if (this.columns != null){
      for (SchemaPath expr : this.columns){
        if ( field.matches(expr)){
          return true;
        }
      }
      return false;
    }
    return true;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    
    columnStatuses = new ArrayList<>();
    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();
    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;
    int columnsToScan = 0;

    MaterializedField field;
    // loop to add up the length of the fixed width columns and build the schema
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      field = MaterializedField.create(toFieldName(column.getPath()),
          toMajorType(column.getType(), getDataMode(column)));
      if ( ! fieldSelected(field)){
        continue;
      }
      columnsToScan++;
      // sum the lengths of all of the fixed length fields
      if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
        // There is not support for the fixed binary type yet in parquet, leaving a task here as a reminder
        // TODO - implement this when the feature is added upstream
//          if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY){
//              byteWidthAllFixedFields += column.getType().getWidth()
//          }
//          else { } // the code below for the rest of the fixed length fields

        bitWidthAllFixedFields += getTypeLengthInBits(column.getType());
      } else {
        allFieldsFixedLength = false;
      }
    }
    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

    // none of the columns in the parquet file matched the request columns from the query
    if (columnsToScan == 0){
      return;
    }
    if (allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(batchSize / bitWidthAllFixedFields, footer.getBlocks().get(0).getColumns().get(0).getValueCount());
    }
    try {
      ArrayList<VarLenBinaryReader.VarLengthColumn> varLengthColumns = new ArrayList<>();
      ArrayList<VarLenBinaryReader.NullableVarLengthColumn> nullableVarLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength = false;
      for (int i = 0; i < columns.size(); ++i) {
        column = columns.get(i);
        columnChunkMetaData = footer.getBlocks().get(0).getColumns().get(i);
        field = MaterializedField.create(toFieldName(column.getPath()),
            toMajorType(column.getType(), getDataMode(column)));
        // the field was not requested to be read
        if ( ! fieldSelected(field)) continue;

        fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
        ValueVector v = TypeHelper.getNewVector(field, allocator);
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          createFixedColumnReader(fieldFixedLength, column, columnChunkMetaData, recordsPerBatch, v);
        } else {
          if (column.getMaxDefinitionLevel() == 0){// column is required
            varLengthColumns.add(new VarLenBinaryReader.VarLengthColumn(this, -1, column, columnChunkMetaData, false, v));
          }
          else{
            nullableVarLengthColumns.add(new VarLenBinaryReader.NullableVarLengthColumn(this, -1, column, columnChunkMetaData, false, v));
          }
        }
      }
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns, nullableVarLengthColumns);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }

    output.removeAllFields();
    try {
      for (ColumnReader crs : columnStatuses) {
        output.addField(crs.valueVecHolder.getValueVector());
      }
      for (VarLenBinaryReader.VarLengthColumn r : varLengthReader.columns) {
        output.addField(r.valueVecHolder.getValueVector());
      }
      for (VarLenBinaryReader.NullableVarLengthColumn r : varLengthReader.nullableColumns) {
        output.addField(r.valueVecHolder.getValueVector());
      }
      output.setNewSchema();
    }catch(SchemaChangeException e) {
      throw new ExecutionSetupException("Error setting up output mutator.", e);
    }

  }

  private SchemaPath toFieldName(String[] paths) {
    return new SchemaPath(Joiner.on('/').join(paths), ExpressionPosition.UNKNOWN);
  }

  private TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (ColumnReader column : columnStatuses) {
      column.valueVecHolder.reset();
      column.valuesReadInCurrentPass = 0;
    }
    for (VarLenBinaryReader.VarLengthColumn r : varLengthReader.columns){
      r.valueVecHolder.reset();
      r.valuesReadInCurrentPass = 0;
    }
    for (VarLenBinaryReader.NullableVarLengthColumn r : varLengthReader.nullableColumns){
      r.valueVecHolder.reset();
      r.valuesReadInCurrentPass = 0;
    }
  }

  /**
   * @param fixedLength
   * @param descriptor
   * @param columnChunkMetaData
   * @param allocateSize - the size of the vector to create
   * @return
   * @throws SchemaChangeException
   */
  private boolean createFixedColumnReader(boolean fixedLength, ColumnDescriptor descriptor,
                                          ColumnChunkMetaData columnChunkMetaData, int allocateSize, ValueVector v)
      throws SchemaChangeException, ExecutionSetupException {
    // if the column is required
    if (descriptor.getMaxDefinitionLevel() == 0){
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        columnStatuses.add(new BitReader(this, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v));
      }
      else{
        columnStatuses.add(new FixedByteAlignedReader(this, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v));
      }
      return true;
    }
    else { // if the column is nullable
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        columnStatuses.add(new NullableBitReader(this, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v));
      }
      else{
        columnStatuses.add(new NullableFixedByteAlignedReader(this, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v));
      }
      return true;
    }
  }

 public void readAllFixedFields(long recordsToRead, ColumnReader firstColumnStatus) throws IOException {

   for (ColumnReader crs : columnStatuses){
     crs.readAllFixedFields(recordsToRead, firstColumnStatus);
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
         firstColumnStatus = varLengthReader.nullableColumns.iterator().next();
        }
      }

      if (allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
      } else {
        recordsToRead = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;

        // going to incorporate looking at length of values and copying the data into a single loop, hopefully it won't
        // get too complicated

        //loop through variable length data to find the maximum records that will fit in this batch
        // this will be a bit annoying if we want to loop though row groups, columns, pages and then individual variable
        // length values...
        // jacques believes that variable length fields will be encoded as |length|value|length|value|...
        // cannot find more information on this right now, will keep looking
      }

//      logger.debug("records to read in this pass: {}", recordsToRead);
      if (allFieldsFixedLength) {
        readAllFixedFields(recordsToRead, firstColumnStatus);
      } else { // variable length columns
        long fixedRecordsToRead = varLengthReader.readFields(recordsToRead, firstColumnStatus);
        readAllFixedFields(fixedRecordsToRead, firstColumnStatus);
      }

      return firstColumnStatus.valuesReadInCurrentPass;
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  static TypeProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                               TypeProtos.DataMode mode) {
    return toMajorType(primitiveTypeName, 0, mode);
  }

  static TypeProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                               TypeProtos.DataMode mode) {
    switch (mode) {

      case OPTIONAL:
        switch (primitiveTypeName) {
          case BINARY:
            return Types.optional(TypeProtos.MinorType.VARBINARY);
          case INT64:
            return Types.optional(TypeProtos.MinorType.BIGINT);
          case INT32:
            return Types.optional(TypeProtos.MinorType.INT);
          case BOOLEAN:
            return Types.optional(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.optional(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.optional(TypeProtos.MinorType.FLOAT8);
          // TODO - Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                .setWidth(length).setMode(mode).build();
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REQUIRED:
        switch (primitiveTypeName) {
          case BINARY:
            return Types.required(TypeProtos.MinorType.VARBINARY);
          case INT64:
            return Types.required(TypeProtos.MinorType.BIGINT);
          case INT32:
            return Types.required(TypeProtos.MinorType.INT);
          case BOOLEAN:
            return Types.required(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.required(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.required(TypeProtos.MinorType.FLOAT8);
          // Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                .setWidth(length).setMode(mode).build();
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REPEATED:
        switch (primitiveTypeName) {
          case BINARY:
            return Types.repeated(TypeProtos.MinorType.VARBINARY);
          case INT64:
            return Types.repeated(TypeProtos.MinorType.BIGINT);
          case INT32:
            return Types.repeated(TypeProtos.MinorType.INT);
          case BOOLEAN:
            return Types.repeated(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.repeated(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.repeated(TypeProtos.MinorType.FLOAT8);
          // Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                .setWidth(length).setMode(mode).build();
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
    }
    throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName + " Mode: " + mode);
  }

  static String join(String delimiter, String... str) {
    StringBuilder builder = new StringBuilder();
    int i = 0;
    for (String s : str) {
      builder.append(s);
      if (i < str.length) {
        builder.append(delimiter);
      }
      i++;
    }
    return builder.toString();
  }

  @Override
  public void cleanup() {
    for (ColumnReader column : columnStatuses) {
      column.clear();
    }
    columnStatuses.clear();

    for (VarLenBinaryReader.VarLengthColumn r : varLengthReader.columns){
      r.clear();
    }
    for (VarLenBinaryReader.NullableVarLengthColumn r : varLengthReader.nullableColumns){
      r.clear();
    }
    varLengthReader.columns.clear();
    varLengthReader.nullableColumns.clear();
  }
}
