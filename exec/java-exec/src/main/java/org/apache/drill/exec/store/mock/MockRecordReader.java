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
package org.apache.drill.exec.store.mock;

import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.mock.MockGroupScanPOP.MockColumn;
import org.apache.drill.exec.store.mock.MockGroupScanPOP.MockScanEntry;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

public class MockRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockRecordReader.class);

  private OutputMutator output;
  private MockScanEntry config;
  private FragmentContext context;
  private BufferAllocator alcator;
  private ValueVector[] valueVectors;
  private int recordsRead;
  private int batchRecordCount;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;


  public MockRecordReader(FragmentContext context, MockScanEntry config) throws OutOfMemoryException {
    this.context = context;
    this.config = config;
    this.fragmentContext=context;
  }

  private int getEstimatedRecordSize(MockColumn[] types) {
    int x = 0;
    for (int i = 0; i < types.length; i++) {
      x += TypeHelper.getSize(types[i].getMajorType());
    }
    return x;
  }

  private MaterializedField getVector(String name, MajorType type, int length) {
    assert context != null : "Context shouldn't be null.";
    MaterializedField f = MaterializedField.create(SchemaPath.getSimplePath(name), type);

    return f;

  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      this.output = output;
      int estimateRowSize = getEstimatedRecordSize(config.getTypes());
      valueVectors = new ValueVector[config.getTypes().length];
      batchRecordCount = 250000 / estimateRowSize;

      for (int i = 0; i < config.getTypes().length; i++) {
        MajorType type = config.getTypes()[i].getMajorType();
        MaterializedField field = getVector(config.getTypes()[i].getName(), type, batchRecordCount);
        Class vvClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
        valueVectors[i] = output.addField(field, vvClass);
      }
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }

  }

  @Override
  public int next() {
    if (recordsRead >= this.config.getRecords()) {
      return 0;
    }

    int recordSetSize = Math.min(batchRecordCount, this.config.getRecords() - recordsRead);

    recordsRead += recordSetSize;
    for (ValueVector v : valueVectors) {

//      logger.debug(String.format("MockRecordReader:  Generating %d records of random data for VV of type %s.", recordSetSize, v.getClass().getName()));
      ValueVector.Mutator m = v.getMutator();
      m.generateTestData(recordSetSize);

    }
    return recordSetSize;
  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public void cleanup() {
  }

}
