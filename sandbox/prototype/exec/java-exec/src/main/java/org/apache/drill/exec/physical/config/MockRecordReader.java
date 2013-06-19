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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MockScanPOP.MockColumn;
import org.apache.drill.exec.physical.config.MockScanPOP.MockScanEntry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos.DataMode;
import org.apache.drill.exec.proto.SchemaDefProtos.MajorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.store.RecordReader;

public class MockRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockRecordReader.class);

  private OutputMutator output;
  private MockScanEntry config;
  private FragmentContext context;
  private ValueVector.Base[] valueVectors;
  private int recordsRead;

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

  private ValueVector.Base getVector(int fieldId, String name, MajorType type, int length) {
    assert context != null : "Context shouldn't be null.";
    if(type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();
    
    MaterializedField f = MaterializedField.create(new SchemaPath(name), fieldId, 0, type);
    ValueVector.Base v;
    v = TypeHelper.getNewVector(f, context.getAllocator());
    v.allocateNew(length);
    return v;

  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      this.output = output;
      int estimateRowSize = getEstimatedRecordSize(config.getTypes());
      valueVectors = new ValueVector.Base[config.getTypes().length];
      int batchRecordCount = 250000 / estimateRowSize;

      for (int i = 0; i < config.getTypes().length; i++) {
        valueVectors[i] = getVector(i, config.getTypes()[i].getName(), config.getTypes()[i].getMajorType(), batchRecordCount);
        output.addField(i, valueVectors[i]);
      }
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }

  }

  @Override
  public int next() {
    int recordSetSize = Math.min(valueVectors[0].capacity(), this.config.getRecords()- recordsRead);
    recordsRead += recordSetSize;
    for(ValueVector.Base v : valueVectors){
      logger.debug("MockRecordReader:  Generating random data for VV of type " + v.getClass().getName());
      v.randomizeData();
      v.setRecordCount(recordSetSize);
    }
    return recordSetSize;
  }

  @Override
  public void cleanup() {
    for (int i = 0; i < valueVectors.length; i++) {
      try {
        output.removeField(valueVectors[i].getField().getFieldId());
      } catch (SchemaChangeException e) {
        logger.warn("Failure while trying to remove field.", e);
      }
      valueVectors[i].close();
    }
  }

}
