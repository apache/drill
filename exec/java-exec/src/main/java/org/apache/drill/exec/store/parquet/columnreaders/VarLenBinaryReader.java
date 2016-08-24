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

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class VarLenBinaryReader {

  ParquetRecordReader parentReader;
  final List<VarLengthColumn<? extends ValueVector>> columns;
  final boolean useAsyncTasks;

  public VarLenBinaryReader(ParquetRecordReader parentReader, List<VarLengthColumn<? extends ValueVector>> columns) {
    this.parentReader = parentReader;
    this.columns = columns;
    useAsyncTasks = parentReader.getFragmentContext().getOptions()
        .getOption(ExecConstants.PARQUET_COLUMNREADER_ASYNC).bool_val;
  }

  /**
   * Reads as many variable length values as possible.
   *
   * @param recordsToReadInThisPass - the number of records recommended for reading form the reader
   * @param firstColumnStatus - a reference to the first column status in the parquet file to grab metatdata from
   * @return - the number of fixed length fields that will fit in the batch
   * @throws IOException
   */
  public long readFields(long recordsToReadInThisPass, ColumnReader<?> firstColumnStatus) throws IOException {

    long recordsReadInCurrentPass = 0;

    // write the first 0 offset
    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.reset();
    }

    //if(useAsyncTasks){
    //  recordsReadInCurrentPass = determineSizesParallel(recordsToReadInThisPass);
    //} else {
      recordsReadInCurrentPass = determineSizesSerial(recordsToReadInThisPass);
    //}
    if(useAsyncTasks){
      readRecordsParallel(recordsReadInCurrentPass);
    }else{
      readRecordsSerial(recordsReadInCurrentPass);
    }
    return recordsReadInCurrentPass;
  }


  private long determineSizesSerial(long recordsToReadInThisPass) throws IOException {
    int lengthVarFieldsInCurrentRecord = 0;
    boolean exitLengthDeterminingLoop = false;
    long totalVariableLengthData = 0;
    long recordsReadInCurrentPass = 0;
    do {
      for (VarLengthColumn<?> columnReader : columns) {
        if (!exitLengthDeterminingLoop) {
          exitLengthDeterminingLoop =
              columnReader.determineSize(recordsReadInCurrentPass, lengthVarFieldsInCurrentRecord);
        } else {
          break;
        }
      }
      // check that the next record will fit in the batch
      if (exitLengthDeterminingLoop ||
          (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields()
              + totalVariableLengthData + lengthVarFieldsInCurrentRecord > parentReader.getBatchSize()) {
        break;
      }
      for (VarLengthColumn<?> columnReader : columns) {
        columnReader.updateReadyToReadPosition();
        columnReader.currDefLevel = -1;
      }
      recordsReadInCurrentPass++;
      totalVariableLengthData += lengthVarFieldsInCurrentRecord;
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);

    return recordsReadInCurrentPass;
  }


  public long determineSizesParallel(long recordsToReadInThisPass ) throws IOException {
    boolean doneReading = false;
    int lengthVarFieldsInCurrentRecord = 0;
    boolean exitLengthDeterminingLoop = false;
    long totalVariableLengthData = 0;
    long recordsReadInCurrentPass = 0;

    do {
    doneReading = readPagesParallel();

    if (!doneReading) {
      lengthVarFieldsInCurrentRecord = 0;
      for (VarLengthColumn<?> columnReader : columns) {
        doneReading = columnReader.processPageData((int) recordsReadInCurrentPass);
        if(doneReading) {
          break;
        }
        lengthVarFieldsInCurrentRecord += columnReader.dataTypeLengthInBits;
        doneReading = columnReader.checkVectorCapacityReached();
        if(doneReading) {
          break;
        }
      }
    }

    exitLengthDeterminingLoop = doneReading;

      // check that the next record will fit in the batch
      if (exitLengthDeterminingLoop ||
          (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields()
              + totalVariableLengthData + lengthVarFieldsInCurrentRecord > parentReader.getBatchSize()) {
        break;
      }
      for (VarLengthColumn<?> columnReader : columns) {
        columnReader.updateReadyToReadPosition();
        columnReader.currDefLevel = -1;
      }
      recordsReadInCurrentPass++;
      totalVariableLengthData += lengthVarFieldsInCurrentRecord;
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);

    return recordsReadInCurrentPass;
  }

  public boolean readPagesParallel() {

    boolean isDone = false;
    ArrayList<Future<Boolean>> futures = Lists.newArrayList();
    for (VarLengthColumn<?> columnReader : columns) {
      Future<Boolean> f = columnReader.readPageAsync();
      futures.add(f);
    }
    for (Future<Boolean> f : futures) {
      try {
        isDone = isDone || f.get().booleanValue();
      } catch (Exception e) {
        f.cancel(true);
        handleAndRaise(null, e);
      }
    }
    return isDone;
  }


  private void readRecordsSerial(long recordsReadInCurrentPass) {
    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.readRecords(columnReader.pageReader.valuesReadyToRead);
    }
    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.valueVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
    }
  }

  private void readRecordsParallel(long recordsReadInCurrentPass){
    ArrayList<Future<Integer>> futures = Lists.newArrayList();
    for (VarLengthColumn<?> columnReader : columns) {
      Future<Integer> f = columnReader.readRecordsAsync(columnReader.pageReader.valuesReadyToRead);
      futures.add(f);
    }
    for (Future f : futures) {
      try {
        f.get();
      } catch (Exception e) {
        f.cancel(true);
        handleAndRaise(null, e);
      }
    }
    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.valueVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    String message = "Error in parquet record reader.\nMessage: " + s;
    throw new DrillRuntimeException(message, e);
  }

}
