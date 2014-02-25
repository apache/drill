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

package org.apache.drill.exec.store.ischema;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;




/**
 * RowRecordReader is a RecordReader which creates RecordBatchs by
 * reading rows one at a time. The fixed format rows come from a "RowProvider".
 */
public class RowRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RowRecordReader.class);

  protected final VectorSet batch;
  protected final RowProvider provider;
  protected final FragmentContext context;
  protected OutputMutator output;
  
  private int bufSize = 32*1024*1024;
  private int maxRowCount;
  /**
   * Construct a RecordReader which uses rows from a RowProvider and puts them into a set of value vectors.
   * @param context
   * @param vectors
   */
  public RowRecordReader(FragmentContext context, VectorSet batch, RowProvider provider) {
    this.context = context;
    this.provider = provider;
    this.batch = batch;
  }
 
  public RowRecordReader(FragmentContext context, SelectedTable table, SchemaPlus rootSchema){
    this.context = context;
    this.provider = table.getProvider(rootSchema);
    this.batch = table.getFixedTable();
  }

  /** 
   * Prepare to create record batches. 
   */
  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output; 
    batch.createVectors(context.getAllocator());
    
    // Inform drill of the output columns. They were set up when the vector handler was created.
    //  Note we are currently working with fixed tables.
    try {
      for (ValueVector v: batch.getValueVectors()) {
        output.addField(v);;
      }
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
    
    // Estimate the number of records we can hold in a RecordBatch
    maxRowCount = batch.getEstimatedRowCount(bufSize);
  }



  /** 
   * Return the next record batch.  An empty batch means end of data.
   */
  @Override
  public int next() {
    
    // Make note are are starting a new batch of records
    batch.beginBatch(maxRowCount);
    
    // Repeat until out of data or vectors are full
    int actualCount;
    for (actualCount = 0; actualCount < maxRowCount && provider.hasNext(); actualCount++) {
   
      // Put the next row into the vectors. If vectors full, try again later.
      Object[] row = provider.next();
      if (!batch.writeRowToVectors(actualCount, row)) {
        provider.previous();
        break;
      }
    }
    
    // Make note the batch is complete. 
    batch.endBatch(actualCount);
    
    // Problem if we had a single row which didn't fit.
    if (actualCount == 0 && provider.hasNext()) {
      throw new DrillRuntimeException("Row size larger than batch size");
    }
    
    // Return the number of rows.  0 means end of data.
    return actualCount;
  }
     

      
  /**
   *  Release all resources 
   */
  public void cleanup() {
    batch.cleanup();
  }


  
}


