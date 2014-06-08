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
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.parquet.FixedByteAlignedReader.Decimal28Reader;
import org.apache.drill.exec.store.parquet.FixedByteAlignedReader.Decimal38Reader;
import org.apache.drill.exec.store.parquet.NullableFixedByteAlignedReader.NullableDecimal28Reader;
import org.apache.drill.exec.store.parquet.NullableFixedByteAlignedReader.NullableDecimal38Reader;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
import parquet.format.FileMetaData;
import parquet.format.SchemaElement;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileWriter;
import parquet.column.Encoding;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.PrimitiveType;

import org.apache.drill.exec.store.parquet.VarLengthColumnReaders.*;

import com.google.common.base.Joiner;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ParquetRecordReader implements RecordReader {
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
    ParquetMetadataConverter metaConverter = new ParquetMetadataConverter();
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
      MajorType mt = toMajorType(column.getType(), se.getType_length(), getDataMode(column), se);
      field = MaterializedField.create(toFieldName(column.getPath()),mt);
      if ( ! fieldSelected(field)){
        continue;
      }
      columnsToScan++;
      // sum the lengths of all of the fixed length fields
      if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
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
    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

    // none of the columns in the parquet file matched the request columns from the query
    if (columnsToScan == 0){
      throw new ExecutionSetupException("Error reading from parquet file. No columns requested were found in the file.");
    }
    if (allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(Math.min(batchSize / bitWidthAllFixedFields,
          footer.getBlocks().get(0).getColumns().get(0).getValueCount()), 65535);
    }
    else {
      recordsPerBatch = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
    }

    try {
      ValueVector v;
      ConvertedType convertedType;
      SchemaElement schemaElement;
      ArrayList<VarLengthColumn> varLengthColumns = new ArrayList<>();
      ArrayList<NullableVarLengthColumn> nullableVarLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength = false;
      for (int i = 0; i < columns.size(); ++i) {
        column = columns.get(i);
        columnChunkMetaData = footer.getBlocks().get(rowGroupIndex).getColumns().get(i);
        schemaElement = schemaElements.get(column.getPath()[0]);
        convertedType = schemaElement.getConverted_type();
        MajorType type = toMajorType(column.getType(), schemaElement.getType_length(), getDataMode(column), schemaElement);
        field = MaterializedField.create(toFieldName(column.getPath()), type);
        // the field was not requested to be read
        if ( ! fieldSelected(field)) continue;

        fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
        v = output.addField(field, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          createFixedColumnReader(fieldFixedLength, column, columnChunkMetaData, recordsPerBatch, v,
            schemaElement);
        } else {
          // create a reader and add it to the appropriate list
          getReader(this, -1, column, columnChunkMetaData, false, v, schemaElement, varLengthColumns, nullableVarLengthColumns);
        }
      }
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns, nullableVarLengthColumns);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private SchemaPath toFieldName(String[] paths) {
    return SchemaPath.getCompoundPath(paths);
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
      column.valuesReadInCurrentPass = 0;
    }
    for (VarLengthColumn r : varLengthReader.columns){
      r.valuesReadInCurrentPass = 0;
    }
    for (NullableVarLengthColumn r : varLengthReader.nullableColumns){
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
                                          ColumnChunkMetaData columnChunkMetaData, int allocateSize, ValueVector v,
                                          SchemaElement schemaElement)
      throws SchemaChangeException, ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();
    // if the column is required
    if (descriptor.getMaxDefinitionLevel() == 0){
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        columnStatuses.add(new BitReader(this, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v, schemaElement));
      } else if (columnChunkMetaData.getType() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && convertedType == ConvertedType.DECIMAL){
        int length = schemaElement.type_length;
        if (length <= 12) {
          columnStatuses.add(new Decimal28Reader(this, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement));
        } else if (length <= 16) {
          columnStatuses.add(new Decimal38Reader(this, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement));
        }
      } else if (columnChunkMetaData.getType() == PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        columnStatuses.add(new FixedByteAlignedReader.DateReader(this, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement));
      } else{
        if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
          columnStatuses.add(new ParquetFixedWidthDictionaryReader(this, allocateSize, descriptor, columnChunkMetaData,
              fixedLength, v, schemaElement));
        } else {
          columnStatuses.add(new FixedByteAlignedReader(this, allocateSize, descriptor, columnChunkMetaData,
              fixedLength, v, schemaElement));
        }
      }
      return true;
    }
    else { // if the column is nullable
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        columnStatuses.add(new NullableBitReader(this, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, v, schemaElement));
      } else if (columnChunkMetaData.getType() == PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        columnStatuses.add(new NullableFixedByteAlignedReader.NullableDateReader(this, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement));
      } else if (columnChunkMetaData.getType() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && convertedType == ConvertedType.DECIMAL){
        int length = schemaElement.type_length;
        if (length <= 12) {
          columnStatuses.add(new NullableDecimal28Reader(this, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement));
        } else if (length <= 16) {
          columnStatuses.add(new NullableDecimal38Reader(this, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement));
        }
      } else {
        columnStatuses.add(NullableFixedByteAlignedReaders.getNullableColumnReader(this, allocateSize, descriptor,
            columnChunkMetaData, fixedLength, v, schemaElement));
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
                                               TypeProtos.DataMode mode, SchemaElement schemaElement) {
    return toMajorType(primitiveTypeName, 0, mode, schemaElement);
  }

  // TODO - move this into ParquetTypeHelper and use code generation to create the list
  static TypeProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                               TypeProtos.DataMode mode, SchemaElement schemaElement) {
    ConvertedType convertedType = schemaElement.getConverted_type();
    switch (mode) {

      case OPTIONAL:
        switch (primitiveTypeName) {
          case BINARY:
            if (convertedType == null) {
              return Types.optional(TypeProtos.MinorType.VARBINARY);
            }
            switch (convertedType) {
              case UTF8:
                return Types.optional(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT64:
            if (convertedType == null) {
              return Types.optional(TypeProtos.MinorType.BIGINT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
              case FINETIME:
                throw new UnsupportedOperationException();
              case TIMESTAMP:
                return Types.optional(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT32:
            if (convertedType == null) {
              return Types.optional(TypeProtos.MinorType.INT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
              case DATE:
                return Types.optional(MinorType.DATE);
              case TIME:
                return Types.optional(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
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
            if (convertedType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                      .setWidth(length).setMode(mode).build();
            } else if (convertedType == ConvertedType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REQUIRED:
        switch (primitiveTypeName) {
          case BINARY:
            if (convertedType == null) {
              return Types.required(TypeProtos.MinorType.VARBINARY);
            }
            switch (convertedType) {
              case UTF8:
                return Types.required(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT64:
            if (convertedType == null) {
              return Types.required(MinorType.BIGINT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
              case FINETIME:
                throw new UnsupportedOperationException();
              case TIMESTAMP:
                return Types.required(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT32:
            if (convertedType == null) {
              return Types.required(MinorType.INT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
              case DATE:
                return Types.required(MinorType.DATE);
              case TIME:
                return Types.required(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
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
            if (convertedType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                      .setWidth(length).setMode(mode).build();
            } else if (convertedType == ConvertedType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REPEATED:
        switch (primitiveTypeName) {
          case BINARY:
            if (convertedType == null) {
              return Types.repeated(TypeProtos.MinorType.VARBINARY);
            }
            switch (schemaElement.getConverted_type()) {
              case UTF8:
                return Types.repeated(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT64:
            if (convertedType == null) {
              return Types.repeated(MinorType.BIGINT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
              case FINETIME:
                throw new UnsupportedOperationException();
              case TIMESTAMP:
                return Types.repeated(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT32:
            if (convertedType == null) {
              return Types.repeated(MinorType.INT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
              case DATE:
                return Types.repeated(MinorType.DATE);
              case TIME:
                return Types.repeated(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
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
            if (convertedType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                      .setWidth(length).setMode(mode).build();
            } else if (convertedType == ConvertedType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
    }
    throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName + " Mode: " + mode);
  }

  private static void getReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v,
                                        SchemaElement schemaElement, List<VarLengthColumn> varLengthColumns,
                                        List<NullableVarLengthColumn> nullableVarLengthColumns) throws ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();
    switch (descriptor.getMaxDefinitionLevel()) {
      case 0:
        if (convertedType == null) {
          varLengthColumns.add(new VarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (VarBinaryVector) v, schemaElement));
          return;
        }
        switch (convertedType) {
          case UTF8:
            varLengthColumns.add(new VarCharColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (VarCharVector) v, schemaElement));
            return;
          case DECIMAL:
            if (v instanceof Decimal28SparseVector) {
              varLengthColumns.add(new Decimal28Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (Decimal28SparseVector) v, schemaElement));
              return;
            } else if (v instanceof Decimal38SparseVector) {
              varLengthColumns.add(new Decimal38Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (Decimal38SparseVector) v, schemaElement));
              return;
            }
          default:
        }
      default:
        if (convertedType == null) {
          nullableVarLengthColumns.add(new NullableVarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement));
          return;
        }
        switch (convertedType) {
          case UTF8:
            nullableVarLengthColumns.add(new NullableVarCharColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarCharVector) v, schemaElement));
            return;
          case DECIMAL:
            if (v instanceof NullableDecimal28SparseVector) {
              nullableVarLengthColumns.add(new NullableDecimal28Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal28SparseVector) v, schemaElement));
              return;
            } else if (v instanceof NullableDecimal38SparseVector) {
              nullableVarLengthColumns.add(new NullableDecimal38Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal38SparseVector) v, schemaElement));
              return;
            }
          default:
        }
    }
    throw new UnsupportedOperationException();
  }

  private static MinorType getDecimalType(SchemaElement schemaElement) {
    return schemaElement.getPrecision() <= 28 ? MinorType.DECIMAL28SPARSE : MinorType.DECIMAL38SPARSE;
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

    for (VarLengthColumn r : varLengthReader.columns){
      r.clear();
    }
    for (NullableVarLengthColumn r : varLengthReader.nullableColumns){
      r.clear();
    }
    varLengthReader.columns.clear();
    varLengthReader.nullableColumns.clear();
  }
}
