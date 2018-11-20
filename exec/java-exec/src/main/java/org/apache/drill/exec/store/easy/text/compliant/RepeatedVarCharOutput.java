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
package org.apache.drill.exec.store.easy.text.compliant;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.RepeatedVarCharVector;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a single vector of type repeated varchar vector. Each record is a single
 * value within the vector containing all the fields in the record as individual array elements.
 */
class RepeatedVarCharOutput extends TextOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedVarCharOutput.class);

  static final String COL_NAME = "columns";
  static final SchemaPath COLUMNS = SchemaPath.getSimplePath("columns");
  public static final int MAXIMUM_NUMBER_COLUMNS = 64 * 1024;

  // output vector
  private final RepeatedVarCharVector vector;

  // mutator for the output vector
  private final RepeatedVarCharVector.Mutator mutator;

  // boolean array indicating which fields are selected (if star query entire array is set to true)
  private final boolean[] collectedFields;

  // pointer to keep track of the offsets per record
  private long repeatedOffset;

  // pointer to keep track of the original offsets per record
  private long repeatedOffsetOriginal;

  // pointer to end of the offset buffer
  private long repeatedOffsetMax;

  // pointer to the start of the actual data buffer
  private long characterDataOriginal;

  // pointer to the current location of the data buffer
  private long characterData;

  // pointer to the end of the data buffer
  private long characterDataMax;

  // current pointer into the buffer that keeps track of the length of individual fields
  private long charLengthOffset;

  // pointer to the start of the length buffer
  private long charLengthOffsetOriginal;

  // pointer to the end of length buffer
  private long charLengthOffsetMax;

  // pointer to the beginning of the record
  private long recordStart;

  // total number of records processed (across batches)
  private long recordCount;

  // number of records processed in this current batch
  private int batchIndex;

  // current index of the field being processed within the record
  private int fieldIndex = -1;

  /* boolean to indicate if we are currently appending data to the output vector
   * Its set to false when we have hit out of memory or we are not interested in
   * the particular field
   */
  private boolean collect;

  // are we currently appending to a field
  private boolean fieldOpen;

  // maximum number of fields/columns
  private final int maxField;

  /**
   * We initialize and add the repeated varchar vector to the record batch in this
   * constructor. Perform some sanity checks if the selected columns are valid or not.
   * @param outputMutator  Used to create/modify schema in the record batch
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @throws SchemaChangeException
   */
  public RepeatedVarCharOutput(OutputMutator outputMutator, Collection<SchemaPath> columns, boolean isStarQuery) throws SchemaChangeException {
    super();

    MaterializedField field = MaterializedField.create(COL_NAME, Types.repeated(TypeProtos.MinorType.VARCHAR));
    this.vector = outputMutator.addField(field, RepeatedVarCharVector.class);

    this.mutator = vector.getMutator();


    { // setup fields
      List<Integer> columnIds = new ArrayList<>();
      if (!isStarQuery) {
        String pathStr;
        for (SchemaPath path : columns) {
          assert path.getRootSegment().isNamed() : "root segment should be named";
          pathStr = path.getRootSegment().getPath();
          Preconditions.checkArgument(COL_NAME.equalsIgnoreCase(pathStr) || (SchemaPath.DYNAMIC_STAR.equals(pathStr) && path.getRootSegment().getChild() == null),
              String.format("Selected column '%s' must have name 'columns' or must be plain '*'", pathStr));

          if (path.getRootSegment().getChild() != null) {
            Preconditions.checkArgument(path.getRootSegment().getChild().isArray(),
              String.format("Selected column '%s' must be an array index", pathStr));
            int index = path.getRootSegment().getChild().getArraySegment().getIndex();
            columnIds.add(index);
          }
        }
        Collections.sort(columnIds);

      }

      boolean[] fields = new boolean[MAXIMUM_NUMBER_COLUMNS];

      int maxField = fields.length;

      if(isStarQuery){
        Arrays.fill(fields, true);
      }else{
        for(Integer i : columnIds){
          maxField = 0;
          maxField = Math.max(maxField, i);
          fields[i] = true;
        }
      }
      this.collectedFields = fields;
      this.maxField = maxField;
    }


  }

  /**
   * Start a new record batch. Resets all the offsets and pointers that
   * store buffer addresses
   */
  @Override
  public void startBatch() {
    this.recordStart = characterDataOriginal;
    this.fieldOpen = false;
    this.batchIndex = 0;
    this.fieldIndex = -1;
    this.collect = true;

    loadRepeatedOffsetAddress();
    loadVarCharOffsetAddress();
    loadVarCharDataAddress();
  }

  private void loadRepeatedOffsetAddress(){
    @SuppressWarnings("resource")
    DrillBuf buf = vector.getOffsetVector().getBuffer();
    checkBuf(buf);
    this.repeatedOffset = buf.memoryAddress() + 4;
    this.repeatedOffsetOriginal = buf.memoryAddress() + 4;
    this.repeatedOffsetMax = buf.memoryAddress() + buf.capacity();
  }

  private void loadVarCharDataAddress(){
    @SuppressWarnings("resource")
    DrillBuf buf = vector.getDataVector().getBuffer();
    checkBuf(buf);
    this.characterData = buf.memoryAddress();
    this.characterDataOriginal = buf.memoryAddress();
    this.characterDataMax = buf.memoryAddress() + buf.capacity();
  }

  private void loadVarCharOffsetAddress(){
    @SuppressWarnings("resource")
    DrillBuf buf = vector.getDataVector().getOffsetVector().getBuffer();
    checkBuf(buf);
    this.charLengthOffset = buf.memoryAddress() + 4;
    this.charLengthOffsetOriginal = buf.memoryAddress() + 4; // add four as offsets conceptually start at 1. (first item is 0..1)
    this.charLengthOffsetMax = buf.memoryAddress() + buf.capacity();
  }

  private void expandVarCharOffsets(){
    vector.getDataVector().getOffsetVector().reAlloc();
    long diff = charLengthOffset - charLengthOffsetOriginal;
    loadVarCharOffsetAddress();
    charLengthOffset += diff;
  }

  private void expandVarCharData(){
    vector.getDataVector().reAlloc();
    long diff = characterData - characterDataOriginal;
    loadVarCharDataAddress();
    characterData += diff;
  }

  private void expandRepeatedOffsets(){
    vector.getOffsetVector().reAlloc();
    long diff = repeatedOffset - repeatedOffsetOriginal;
    loadRepeatedOffsetAddress();
    repeatedOffset += diff;
  }

  /**
   * Helper method to check if the buffer we are accessing
   * has a minimum reference count and has not been deallocated
   * @param b  working drill buffer
   */
  private void checkBuf(DrillBuf b){
    if(b.refCnt() < 1){
      throw new IllegalStateException("Cannot access a dereferenced buffer.");
    }
  }

  @Override
  public void startField(int index) {
    fieldIndex = index;
    collect = collectedFields[index];
    fieldOpen = true;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;

    if(charLengthOffset >= charLengthOffsetMax){
      expandVarCharOffsets();
    }

    int newOffset = (int) (characterData - characterDataOriginal);
    PlatformDependent.putInt(charLengthOffset, newOffset);
    charLengthOffset += 4;
    return fieldIndex < maxField;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void append(byte data) {
    if(!collect){
      return;
    }

    if(characterData >= characterDataMax){
      expandVarCharData();
    }

    PlatformDependent.putByte(characterData, data);
    characterData++;

  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean rowHasData() {
    return this.recordStart < characterData;
  }

  @Override
  public void finishRecord() {
    this.recordStart = characterData;

    if(fieldOpen){
      endField();
    }

    if(repeatedOffset >= repeatedOffsetMax){
      expandRepeatedOffsets();
    }

    int newOffset = ((int) (charLengthOffset - charLengthOffsetOriginal))/4;
    PlatformDependent.putInt(repeatedOffset, newOffset);
    repeatedOffset += 4;

    // if there were no defined fields, skip.
    if(fieldIndex > -1){
      batchIndex++;
      recordCount++;
    }


  }

  /**
   * This method is a helper method added for DRILL-951
   * TextRecordReader to call this method to get field names out
   * @return array of field data strings
   */
  public String [] getTextOutput () throws ExecutionSetupException {
    if (recordCount == 0 || fieldIndex == -1) {
      return null;
    }

    if (this.recordStart != characterData) {
      throw new ExecutionSetupException("record text was requested before finishing record");
    }

    //Currently only first line header is supported. Return only first record.
    int retSize = fieldIndex+1;
    String [] out = new String [retSize];

    RepeatedVarCharVector.Accessor a = this.vector.getAccessor();
    for (int i=0; i<retSize; i++){
      out[i] = a.getSingleObject(0,i).toString();
    }
    return out;
  }

  @Override
  public void finishBatch() { }
}
