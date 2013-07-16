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
package org.apache.drill.exec.physical.config;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MockScanPOP.MockColumn;
import org.apache.drill.exec.physical.config.MockScanPOP.MockScanEntry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NonRepeatedMutator;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

public class MockRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockRecordReader.class);

  private OutputMutator output;
  private MockScanEntry config;
  private FragmentContext context;
  private ValueVector[] valueVectors;
  private int recordsRead;
  private int batchRecordCount;

  public MockRecordReader(FragmentContext context, MockScanEntry config) {
    this.context = context;
    this.config = config;
  }

  private int getEstimatedRecordSize(MockColumn[] types) {
    int x = 0;
    for (int i = 0; i < types.length; i++) {
      x += TypeHelper.getSize(types[i].getMajorType());
    }
    return x;
  }

  private ValueVector getVector(String name, MajorType type, int length) {
    assert context != null : "Context shouldn't be null.";
    if(type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();
    
    MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
    ValueVector v;
    v = TypeHelper.getNewVector(f, context.getAllocator());
    if(v instanceof FixedWidthVector){
      ((FixedWidthVector)v).allocateNew(length);  
    }else if(v instanceof VariableWidthVector){
      ((VariableWidthVector)v).allocateNew(50*length, length);
    }else{
      throw new UnsupportedOperationException(String.format("Unable to get allocate vector %s", v.getClass().getName()));
    }
    
    return v;

  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      this.output = output;
      int estimateRowSize = getEstimatedRecordSize(config.getTypes());
      valueVectors = new ValueVector[config.getTypes().length];
      batchRecordCount = 250000 / estimateRowSize;

      for (int i = 0; i < config.getTypes().length; i++) {
        valueVectors[i] = getVector(config.getTypes()[i].getName(), config.getTypes()[i].getMajorType(), batchRecordCount);
        output.addField(valueVectors[i]);
      }
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }

  }

  @Override
  public int next() {
    
    int recordSetSize = Math.min(batchRecordCount, this.config.getRecords()- recordsRead);

    recordsRead += recordSetSize;
    for(ValueVector v : valueVectors){
      if(v instanceof FixedWidthVector){
        ((FixedWidthVector)v).allocateNew(recordSetSize);
      }else if(v instanceof VariableWidthVector){
        ((VariableWidthVector)v).allocateNew(50*recordSetSize, recordSetSize);
      }else{
        throw new UnsupportedOperationException();
      }
      
      logger.debug("MockRecordReader:  Generating random data for VV of type " + v.getClass().getName());
      ValueVector.Mutator m = v.getMutator();
      m.randomizeData();
      
      if(m instanceof NonRepeatedMutator){
        ((NonRepeatedMutator)m).setValueCount(recordSetSize);  
      }else{
        throw new UnsupportedOperationException();
      }
      
    }
    return recordSetSize;
  }

  @Override
  public void cleanup() {
    for (int i = 0; i < valueVectors.length; i++) {
      try {
        output.removeField(valueVectors[i].getField());
      } catch (SchemaChangeException e) {
        logger.warn("Failure while trying to remove field.", e);
      }
      valueVectors[i].close();
    }
  }

}
