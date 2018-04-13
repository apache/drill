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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.VarCharVector;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of varchar vectors. A varchar vector contains all the field
 * values for a given column. Each record is a single value within each vector of the set.
 */
class FieldVarCharOutput extends TextOutput {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldVarCharOutput.class);
  static final String COL_NAME = "columns";

  // array of output vector
  private final VarCharVector [] vectors;
  // boolean array indicating which fields are selected (if star query entire array is set to true)
  private final boolean[] selectedFields;
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

  private boolean collect = true;
  private boolean rowHasData= false;
  private static final int MAX_FIELD_LENGTH = 1024 * 64;
  private int recordCount = 0;
  private int maxField = 0;
  private int[] nullCols;
  private byte nullValue[] = new byte[0];

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

    int totalFields = fieldNames.length;
    List<String> outputColumns = new ArrayList<>(Arrays.asList(fieldNames));
    List<Integer> nullColumns = new ArrayList<>();

    if (isStarQuery) {
      maxField = totalFields - 1;
      this.selectedFields = new boolean[totalFields];
      Arrays.fill(selectedFields, true);
    } else {
      List<Integer> columnIds = new ArrayList<Integer>();
      Map<String, Integer> headers = CaseInsensitiveMap.newHashMap();
      for (int i = 0; i < fieldNames.length; i++) {
        headers.put(fieldNames[i], i);
      }

      for (SchemaPath path : columns) {
        int index;
        String pathStr = path.getRootSegment().getPath();
        if (pathStr.equals(COL_NAME) && path.getRootSegment().getChild() != null) {
          //TODO: support both field names and columns index along with predicate pushdown
          throw UserException
              .unsupportedError()
              .message("With extractHeader enabled, only header names are supported")
              .addContext("column name", pathStr)
              .addContext("column index", path.getRootSegment().getChild())
              .build(logger);
        } else {
          Integer value = headers.get(pathStr);
          if (value == null) {
            // found col that is not a part of fieldNames, add it
            // this col might be part of some another scanner
            index = totalFields++;
            outputColumns.add(pathStr);
            nullColumns.add(index);
          } else {
            index = value;
          }
        }
        columnIds.add(index);
      }
      Collections.sort(columnIds);

      this.selectedFields = new boolean[totalFields];
      for(Integer i : columnIds) {
        selectedFields[i] = true;
        maxField = i;
      }
    }

    this.vectors = new VarCharVector[totalFields];

    for (int i = 0; i <= maxField; i++) {
      if (selectedFields[i]) {
        MaterializedField field = MaterializedField.create(outputColumns.get(i), Types.required(TypeProtos.MinorType.VARCHAR));
        this.vectors[i] = outputMutator.addField(field, VarCharVector.class);
      }
    }

    this.fieldBytes = new byte[MAX_FIELD_LENGTH];

    // Keep track of the null columns to be filled in.

    nullCols = new int[nullColumns.size()];
    for (int i = 0; i < nullCols.length; i++) {
      nullCols[i] = nullColumns.get(i);
    }
  }

  /**
   * Start a new record batch. Resets all pointers
   */
  @Override
  public void startBatch() {
    recordCount = 0;
    currentFieldIndex= -1;
    collect = true;
    fieldOpen = false;
  }

  @Override
  public void startField(int index) {
    currentFieldIndex = index;
    currentDataPointer = 0;
    fieldOpen = true;
    collect = selectedFields[index];
    currentVector = vectors[index];
  }

  @Override
  public void append(byte data) {
    if (!collect) {
      return;
    }

    if (currentDataPointer >= MAX_FIELD_LENGTH -1) {
      throw UserException
          .unsupportedError()
          .message("Trying to write something big in a column")
          .addContext("columnIndex", currentFieldIndex)
          .addContext("Limit", MAX_FIELD_LENGTH)
          .build(logger);
    }

    fieldBytes[currentDataPointer++] = data;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;

    if (collect) {
      assert currentVector != null;
      currentVector.getMutator().setSafe(recordCount, fieldBytes, 0, currentDataPointer);
    }

    if (currentDataPointer > 0) {
      this.rowHasData = true;
    }

    return currentFieldIndex < maxField;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

 @Override
  public void finishRecord() {
    if (fieldOpen){
      endField();
    }

    // Fill in null (really empty) values.

    for (int i = 0; i < nullCols.length; i++) {
      vectors[nullCols[i]].getMutator().setSafe(recordCount, nullValue, 0, 0);
    }
    recordCount++;
  }

  @Override
  public void finishBatch() { }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean rowHasData() {
    return this.rowHasData;
  }
}
