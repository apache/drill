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

import static junit.framework.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.SettableFuture;

public class ParquetResultListener implements UserResultsListener {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetResultListener.class);

  private SettableFuture<Void> future = SettableFuture.create();
  int count = 0;
  int totalRecords;

  boolean testValues;
  BufferAllocator allocator;

  int batchCounter = 1;
  HashMap<String, Integer> valuesChecked = new HashMap<>();
  ParquetTestProperties props;

  ParquetResultListener(BufferAllocator allocator, ParquetTestProperties props, int numberOfTimesRead, boolean testValues){
    this.allocator = allocator;
    this.props = props;
    this.totalRecords = props.recordsPerRowGroup * props.numberRowGroups * numberOfTimesRead;
    this.testValues = testValues;
  }

  @Override
  public void submissionFailed(RpcException ex) {
    logger.error("Submission failed.", ex);
    future.setException(ex);
  }


  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, Object value, String name) {
    assertField(valueVector, index, expectedMinorType, value, name, 0);
  }

  @SuppressWarnings("unchecked")
  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, T value, String name, int parentFieldId) {

    if (expectedMinorType == TypeProtos.MinorType.MAP) {
      return;
    }

    T val;
    try {
    val = (T) valueVector.getAccessor().getObject(index);
    if (val instanceof byte[]) {
      assert(Arrays.equals((byte[]) value, (byte[]) val));
    }
    else if (val instanceof String) {
      assert(val.equals(value));
    } else {
      assertEquals(value, val);
    }
    } catch (Throwable ex) {
      throw ex;
    }
  }

  @Override
  synchronized public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
    logger.debug("result arrived in test batch listener.");
    if(result.getHeader().getIsLastChunk()){
      future.set(null);
    }
    int columnValCounter = 0;
    FieldInfo currentField;
    count += result.getHeader().getRowCount();
    boolean schemaChanged = false;
    RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    try {
      schemaChanged = batchLoader.load(result.getHeader().getDef(), result.getData());
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }

    // used to make sure each vector in the batch has the same number of records
    int valueCount = batchLoader.getRecordCount();

    // print headers.
    if (schemaChanged) {
    } // do not believe any change is needed for when the schema changes, with the current mock scan use case

    for (VectorWrapper vw : batchLoader) {
      ValueVector vv = vw.getValueVector();
      currentField = props.fields.get(vv.getField().getPath().getRootSegment().getPath());
      if ( ! valuesChecked.containsKey(vv.getField().getPath().getRootSegment().getPath())){
        valuesChecked.put(vv.getField().getPath().getRootSegment().getPath(), 0);
        columnValCounter = 0;
      } else {
        columnValCounter = valuesChecked.get(vv.getField().getPath().getRootSegment().getPath());
      }
      printColumnMajor(vv);

      if (testValues){
        for (int j = 0; j < vv.getAccessor().getValueCount(); j++) {
          assertField(vv, j, currentField.type,
              currentField.values[columnValCounter % 3], currentField.name + "/");
          columnValCounter++;
        }
      } else {
        columnValCounter += vv.getAccessor().getValueCount();
      }

      valuesChecked.remove(vv.getField().getPath().getRootSegment().getPath());
      assertEquals("Mismatched value count for vectors in the same batch.", valueCount, vv.getAccessor().getValueCount());
      valuesChecked.put(vv.getField().getPath().getRootSegment().getPath(), columnValCounter);
    }

    if (ParquetRecordReaderTest.VERBOSE_DEBUG){
      printRowMajor(batchLoader);
    }
    batchCounter++;
    if(result.getHeader().getIsLastChunk()){
      checkLastChunk(batchLoader, result);
    }

    batchLoader.clear();
    result.release();
  }

  public void checkLastChunk(RecordBatchLoader batchLoader, QueryResultBatch result) {
    int recordsInBatch = -1;
    // ensure the right number of columns was returned, especially important to ensure selective column read is working
    if (testValues) {
      assertEquals( "Unexpected number of output columns from parquet scan.", props.fields.keySet().size(), valuesChecked.keySet().size() );
    }
    for (String s : valuesChecked.keySet()) {
      try {
        if (recordsInBatch == -1 ){
          recordsInBatch = valuesChecked.get(s);
        } else {
          assertEquals("Mismatched record counts in vectors.", recordsInBatch, valuesChecked.get(s).intValue());
        }
        assertEquals("Record count incorrect for column: " + s, totalRecords, (long) valuesChecked.get(s));
      } catch (AssertionError e) { submissionFailed(new RpcException(e)); }
    }

    assert valuesChecked.keySet().size() > 0;
    batchLoader.clear();
    result.release();
    future.set(null);
  }

  public void printColumnMajor(ValueVector vv) {
    if (ParquetRecordReaderTest.VERBOSE_DEBUG){
      System.out.println("\n" + vv.getField().getAsSchemaPath().getRootSegment().getPath());
    }
    for (int j = 0; j < vv.getAccessor().getValueCount(); j++) {
      if (ParquetRecordReaderTest.VERBOSE_DEBUG){
        Object o = vv.getAccessor().getObject(j);
        if (o instanceof byte[]) {
          try {
            o = new String((byte[])o, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
          }
        }
        System.out.print(Strings.padStart(o + "", 20, ' ') + " ");
        System.out.print(", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - " : ""));
      }
    }
    if (ParquetRecordReaderTest.VERBOSE_DEBUG){
      System.out.println("\n" + vv.getAccessor().getValueCount());
    }
  }

  public void printRowMajor(RecordBatchLoader batchLoader) {
    for (int i = 0; i < batchLoader.getRecordCount(); i++) {
      if (i % 50 == 0){
        System.out.println();
        for (VectorWrapper vw : batchLoader) {
          ValueVector v = vw.getValueVector();
          System.out.print(Strings.padStart(v.getField().getAsSchemaPath().getRootSegment().getPath(), 20, ' ') + " ");

        }
        System.out.println();
        System.out.println();
      }

      for (VectorWrapper vw : batchLoader) {
        ValueVector v = vw.getValueVector();
        Object o = v.getAccessor().getObject(i);
        if (o instanceof byte[]) {
          try {
            // TODO - in the dictionary read error test there is some data that does not look correct
            // the output of our reader matches the values of the parquet-mr cat/head tools (no full comparison was made,
            // but from a quick check of a few values it looked consistent
            // this might have gotten corrupted by pig somehow, or maybe this is just how the data is supposed ot look
            // TODO - check this!!
//              for (int k = 0; k < ((byte[])o).length; k++ ) {
//                // check that the value at each position is a valid single character ascii value.
//
//                if (((byte[])o)[k] > 128) {
//                  System.out.println("batch: " + batchCounter + " record: " + recordCount);
//                }
//              }
            o = new String((byte[])o, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
          }
        }
        System.out.print(Strings.padStart(o + "", 20, ' ') + " ");
      }
      System.out.println();
    }
  }

  public void getResults() throws RpcException{
    try{
      future.get();
    }catch(Throwable t){
      throw RpcException.mapException(t);
    }
  }

  @Override
  public void queryIdArrived(UserBitShared.QueryId queryId) {
  }
}
