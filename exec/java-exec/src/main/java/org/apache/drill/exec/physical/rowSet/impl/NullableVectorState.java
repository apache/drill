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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.ValuesVectorState;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.NullableScalarWriter;

public class NullableVectorState implements VectorState {

  public static class BitsVectorState extends ValuesVectorState {

    public BitsVectorState(ColumnMetadata schema, AbstractScalarWriter writer, ValueVector mainVector) {
      super(schema, writer, mainVector);
    }

    @Override
    public int allocateVector(ValueVector vector, int cardinality) {
      ((FixedWidthVector) vector).allocateNew(cardinality);
      return vector.getBufferSize();
    }
  }

  private final ColumnMetadata schema;
  private final NullableScalarWriter writer;
  private final NullableVector vector;
  private final ValuesVectorState bitsState;
  private final ValuesVectorState valuesState;

  public NullableVectorState(AbstractObjectWriter writer, NullableVector vector) {
    this.schema = writer.schema();
    this.vector = vector;

    this.writer = (NullableScalarWriter) writer.scalar();
    bitsState = new BitsVectorState(schema, this.writer.bitsWriter(), vector.getBitsVector());
    valuesState = new ValuesVectorState(schema, this.writer.baseWriter(), vector.getValuesVector());
  }

  @Override
  public int allocate(int cardinality) {
    return bitsState.allocate(cardinality) +
           valuesState.allocate(cardinality);
  }

  @Override
  public void rollover(int cardinality) {
    bitsState.rollover(cardinality);
    valuesState.rollover(cardinality);
  }

  @Override
  public void harvestWithLookAhead() {
    bitsState.harvestWithLookAhead();
    valuesState.harvestWithLookAhead();
  }

  @Override
  public void startBatchWithLookAhead() {
    bitsState.startBatchWithLookAhead();
    valuesState.startBatchWithLookAhead();
  }

  @Override
  public void reset() {
    bitsState.reset();
    valuesState.reset();
  }

  @Override
  public ValueVector vector() { return vector; }

  @Override
  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("schema", schema)
      .attributeIdentity("writer", writer)
      .attributeIdentity("vector", vector)
      .attribute("bitsState");
    bitsState.dump(format);
    format
      .attribute("valuesState");
    valuesState.dump(format);
    format
      .endObject();
  }
}
