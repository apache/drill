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
package org.apache.drill.exec.store.easy.text.compliant;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.VarCharVector;

import java.util.Collection;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of varchar vectors. A varchar vector contains all the field
 * values for a given column. Each record is a single value within each vector of the set.
 */
class FieldVarCharOutput extends TextOutput {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldVarCharOutput.class);

  // array of output vector
  private final VarCharVector [] vectors;
  // current vector to which field will be added
  private VarCharVector currentVector;
  // track which field is getting appended
  private int currentFieldIndex = -1;
  // track chars within field
  private int currentDataPointer = 0;
  // track if field is still getting appended
  private boolean fieldOpen = true;
  // holds chars for a field
  private byte[] fieldBytes;

  private boolean rowHasData= false;
  private static final int MAX_FIELD_LENGTH = 1024 * 10;
  private static final int MAXIMUM_NUMBER_COLUMNS = 64 * 1024;
  private int recordCount = 0;
  private int batchIndex = 0;
  private int numFields = 0;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @throws SchemaChangeException
   */
  public FieldVarCharOutput(OutputMutator outputMutator, String [] fieldNames, Collection<SchemaPath> columns, boolean isStarQuery) throws SchemaChangeException {

    numFields = fieldNames.length;
    this.vectors = new VarCharVector[numFields];

    for (int i = 0; i < numFields; i++) {
      MaterializedField field = MaterializedField.create(fieldNames[i], Types.required(TypeProtos.MinorType.VARCHAR));
      this.vectors[i] = outputMutator.addField(field, VarCharVector.class);
    }

    this.fieldBytes = new byte[MAX_FIELD_LENGTH];
  }

  /**
   * Start a new record batch. Resets all pointers
   */
  @Override
  public void startBatch() {
    this.recordCount = 0;
    this.batchIndex = 0;
    this.currentFieldIndex= -1;
    this.fieldOpen = false;
  }

  @Override
  public void startField(int index) {
    currentFieldIndex = index;
    currentDataPointer = 0;
    fieldOpen = true;
    currentVector = vectors[index];
  }

  @Override
  public void append(byte data) {
    if (currentDataPointer >= MAX_FIELD_LENGTH -1) {
      //TODO: figure out how to handle this
    }
    fieldBytes[currentDataPointer++] = data;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;
    currentVector.getMutator().setSafe(recordCount, fieldBytes, 0, currentDataPointer);

    if (currentDataPointer > 0) {
      this.rowHasData = true;
    }

    return currentFieldIndex < MAXIMUM_NUMBER_COLUMNS;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

 @Override
  public void finishRecord() {
    if(fieldOpen){
      endField();
    }

    recordCount++;
  }

  // Sets the record count in this batch within the value vector
  @Override
  public void finishBatch() {
    batchIndex++;

    for (int i = 0; i < numFields; i++) {
      this.vectors[i].getMutator().setValueCount(batchIndex);
    }
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean rowHasData() {
    return this.rowHasData;
  }

 }
