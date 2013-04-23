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
package org.apache.drill.exec.store;

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.exec.exception.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OutputMutator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.vector.Int16Vector;
import org.apache.drill.exec.record.vector.Int32Vector;
import org.apache.drill.exec.record.vector.ValueVector;

public class MockRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockRecordReader.class);

  private BatchSchema expectedSchema;
  private OutputMutator output;
  private MockRecordConfig config;
  private FragmentContext context;
  private ValueVector<?>[] valueVectors;
  private int recordsRead;

  public MockRecordReader(FragmentContext context, MockRecordConfig config) {
    this.config = config;
  }

  private int getEstimatedRecordSize(DataType[] types) {
    int x = 0;
    for (int i = 0; i < types.length; i++) {
      x += getEstimatedColumnSize(i);
    }
    return x;
  }

  private int getEstimatedColumnSize(int fieldId) {
    return 4;
  }

  private ValueVector<?> getVector(int fieldId, DataType dt, int length) {
    ValueVector<?> v;
    if (dt == DataType.INT16) {
      v = new Int16Vector(fieldId, context.getAllocator());
    } else if (dt == DataType.INT32) {
      v = new Int32Vector(fieldId, context.getAllocator());
    } else {
      throw new UnsupportedOperationException();
    }
    v.allocateNew(length);
    return v;

  }

  @Override
  public void setup(BatchSchema expectedSchema, OutputMutator output) throws ExecutionSetupException {
    try {
      this.expectedSchema = expectedSchema;
      this.output = output;
      int estimateRowSize = getEstimatedRecordSize(config.getTypes());
      valueVectors = new ValueVector<?>[config.getTypes().length];
      int batchRecordCount = 250000 / estimateRowSize;

      for (int i = 0; i < config.getTypes().length; i++) {
        valueVectors[i] = getVector(i, config.getTypes()[i], batchRecordCount);
        output.addField(i, valueVectors[i]);
      }
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }

  }

  @Override
  public int next() {
    int recordSetSize = Math.min(valueVectors[0].size(), this.config.getRecordCount()- recordsRead);
    recordsRead += recordSetSize;
    return recordSetSize;
  }

  @Override
  public void cleanup() {
    for (int i = 0; i < valueVectors.length; i++) {
      try {
        output.removeField(valueVectors[i].getField().getFieldId());
      } catch (SchemaChangeException e) {
        logger.warn("Failure while trying tremove field.", e);
      }
      valueVectors[i].close();
    }
  }

}
