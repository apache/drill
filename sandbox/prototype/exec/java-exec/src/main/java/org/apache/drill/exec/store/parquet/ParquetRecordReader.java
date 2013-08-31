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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;

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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import com.google.common.base.Joiner;

public class ParquetRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final int NUMBER_OF_VECTORS = 1;
  private static final long DEFAULT_BATCH_LENGTH = 256 * 1024 * NUMBER_OF_VECTORS; // 256kb
  private static final long DEFAULT_BATCH_LENGTH_IN_BITS = DEFAULT_BATCH_LENGTH * 8; // 256kb

  // TODO - should probably find a smarter way to set this, currently 2 megabytes
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
  private ByteBuf bufferWithAllData;
  private final FieldReference ref;
  long totalRecords;
  long rowGroupOffset;

  private List<ColumnReader> columnStatuses;
  FileSystem fileSystem;
  private BufferAllocator allocator;
  private long batchSize;
  Path hadoopPath;
  private final VarLenBinaryReader varLengthReader;

  public CodecFactoryExposer getCodecFactoryExposer() {
    return codecFactoryExposer;
  }

  private final CodecFactoryExposer codecFactoryExposer;

  int rowGroupIndex;

  public ParquetRecordReader(FragmentContext fragmentContext,
                             String path, int rowGroupIndex, FileSystem fs,
                             CodecFactoryExposer codecFactoryExposer, ParquetMetadata footer, FieldReference ref) throws ExecutionSetupException {
    this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, path, rowGroupIndex, fs, codecFactoryExposer, footer, ref);
  }


  public ParquetRecordReader(FragmentContext fragmentContext, long batchSize,
                             String path, int rowGroupIndex, FileSystem fs,
                             CodecFactoryExposer codecFactoryExposer, ParquetMetadata footer, FieldReference ref) throws ExecutionSetupException {
    this.allocator = fragmentContext.getAllocator();

    hadoopPath = new Path(path);
    fileSystem = fs;
    this.ref = ref;
    this.codecFactoryExposer = codecFactoryExposer;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;

    columnStatuses = new ArrayList<>();

    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();

    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;

    // loop to add up the length of the fixed width columns and build the schema
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      logger.debug("Found Parquet column {} of type {}", column.getPath(), column.getType());
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

    if (allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(batchSize / bitWidthAllFixedFields, footer.getBlocks().get(0).getColumns().get(0).getValueCount());
    }
    try {
      ArrayList<VarLenBinaryReader.VarLengthColumn> varLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength = false;
      MaterializedField field;
      for (int i = 0; i < columns.size(); ++i) {
        column = columns.get(i);
        columnChunkMetaData = footer.getBlocks().get(0).getColumns().get(i);
        field = MaterializedField.create(toFieldName(column.getPath()),
            toMajorType(column.getType(), getDataMode(column, footer.getFileMetaData().getSchema())));
        fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
        ValueVector v = TypeHelper.getNewVector(field, allocator);
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          createFixedColumnReader(fieldFixedLength, field, column, columnChunkMetaData, recordsPerBatch, v);
        } else {
          varLengthColumns.add(new VarLenBinaryReader.VarLengthColumn(this, -1, column, columnChunkMetaData, false, v));
        }
      }
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  public ByteBuf getBufferWithAllData() {
    return bufferWithAllData;
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

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    output.removeAllFields();

    try {
      for (ColumnReader crs : columnStatuses) {
        output.addField(crs.valueVecHolder.getValueVector());
      }
      for (VarLenBinaryReader.VarLengthColumn r : varLengthReader.columns) {
        output.addField(r.valueVecHolder.getValueVector());
      }
      output.setNewSchema();
    }catch(SchemaChangeException e) {
      throw new ExecutionSetupException("Error setting up output mutator.", e);
    }

    // the method for reading into a ByteBuf from a stream copies all of the data into a giant buffer
    // here we do the same thing in a loop to not initialize so much on heap

    // TODO - this should be replaced by an enhancement in Hadoop 2.0 that will allow reading
    // directly into a ByteBuf passed into the reading method
    int totalByteLength = 0;
    long start = 0;
    if (rowGroupIndex == 0){
      totalByteLength = 4;
    }
    else{
      start = rowGroupOffset;
    }
    // TODO - the methods for get total size and get total uncompressed size seem to have the opposite results of
    // what they should
    // I found the bug in the mainline and made a issue for it, hopefully it will be fixed soon
    for (ColumnReader crs : columnStatuses){
      totalByteLength += crs.columnChunkMetaData.getTotalSize();
    }
    for (VarLenBinaryReader.VarLengthColumn r : varLengthReader.columns){
      totalByteLength += r.columnChunkMetaData.getTotalSize();
    }
    int bufferSize = 64*1024;
    long totalBytesWritten = 0;
    int validBytesInCurrentBuffer;
    byte[] buffer = new byte[bufferSize];
    try {
      bufferWithAllData = allocator.buffer(totalByteLength);
      FSDataInputStream inputStream = fileSystem.open(hadoopPath);
      inputStream.seek(start);
      while (totalBytesWritten < totalByteLength){
        validBytesInCurrentBuffer = (int) Math.min(bufferSize, totalByteLength - totalBytesWritten);
        inputStream.read(buffer, 0 , validBytesInCurrentBuffer);
        bufferWithAllData.writeBytes(buffer, 0 , (int) validBytesInCurrentBuffer);
        totalBytesWritten += validBytesInCurrentBuffer;
      }

    } catch (IOException e) {
      throw new ExecutionSetupException("Error opening or reading metatdata for parquet file at location: " + hadoopPath.getName());
    }
  }

  private SchemaPath toFieldName(String[] paths) {
    if(this.ref == null){
      return new SchemaPath(Joiner.on('/').join(paths), ExpressionPosition.UNKNOWN);
    }else{
      return ref.getChild(paths);
    }
    
  }

  private TypeProtos.DataMode getDataMode(ColumnDescriptor column, MessageType schema) {
    if (schema.getColumnDescription(column.getPath()).getMaxDefinitionLevel() == 0) {
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
  }

  /**
   * @param fixedLength
   * @param field
   * @param descriptor
   * @param columnChunkMetaData
   * @param allocateSize - the size of the vector to create
   * @return
   * @throws SchemaChangeException
   */
  private boolean createFixedColumnReader(boolean fixedLength, MaterializedField field, ColumnDescriptor descriptor,
                                          ColumnChunkMetaData columnChunkMetaData, int allocateSize, ValueVector v)
      throws SchemaChangeException {
    TypeProtos.MajorType type = field.getType();
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
      ColumnReader firstColumnStatus = columnStatuses.iterator().next();
      if (allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
      } else {
        // arbitrary
        recordsToRead = 8000;

        // going to incorporate looking at length of values and copying the data into a single loop, hopefully it won't
        // get too complicated

        //loop through variable length data to find the maximum records that will fit in this batch
        // this will be a bit annoying if we want to loop though row groups, columns, pages and then individual variable
        // length values...
        // jacques believes that variable length fields will be encoded as |length|value|length|value|...
        // cannot find more information on this right now, will keep looking
      }

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
    columnStatuses.clear();
    this.varLengthReader.columns.clear();
    bufferWithAllData.release();
  }
}
