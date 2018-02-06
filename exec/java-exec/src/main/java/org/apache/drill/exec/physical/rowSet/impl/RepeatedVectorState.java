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

import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.ValuesVectorState;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Vector state for a scalar array (repeated scalar) vector. Manages both the
 * offsets vector and data vector during overflow and other operations.
 */

public class RepeatedVectorState implements VectorState {
  private final ColumnMetadata schema;
  private final AbstractArrayWriter arrayWriter;
  private final RepeatedValueVector vector;
  private final OffsetVectorState offsetsState;
  private final ValuesVectorState valuesState;

  public RepeatedVectorState(AbstractObjectWriter writer, RepeatedValueVector vector) {
    this.schema = writer.schema();

    // Get the repeated vector

    this.vector = vector;

    // Create the values state using the value (data) portion of the repeated
    // vector, and the scalar (value) portion of the array writer.

    arrayWriter = (AbstractArrayWriter) writer.array();
    AbstractScalarWriter colWriter = (AbstractScalarWriter) arrayWriter.scalar();
    valuesState = new ValuesVectorState(schema, colWriter, vector.getDataVector());

    // Create the offsets state with the offset vector portion of the repeated
    // vector, and the offset writer portion of the array writer.

    offsetsState = new OffsetVectorState(arrayWriter.offsetWriter(),
        vector.getOffsetVector(),
        (AbstractObjectWriter) arrayWriter.entry());
  }

  @Override
  public ValueVector vector() { return vector; }

  @Override
  public int allocate(int cardinality) {
    return offsetsState.allocate(cardinality) +
           valuesState.allocate(childCardinality(cardinality));
  }

  private int childCardinality(int cardinality) {
    return cardinality * schema.expectedElementCount();
  }

  /**
   * The column is a scalar or an array of scalars. We need to roll over both the column
   * values and the offsets that point to those values. The index provided is
   * the index into the offset vector. We use that to obtain the index of the
   * values to roll-over.
   * <p>
   * Data structure:
   * <p><pre></code>
   * RepeatedVectorState (this class)
   * +- OffsetVectorState
   * .  +- OffsetVectorWriter (A)
   * .  +- Offset vector (B)
   * .  +- Backup (e.g. look-ahead) offset vector
   * +- ValuesVectorState
   * .  +- Scalar (element) writer (C)
   * .  +- Data (elements) vector (D)
   * .  +- Backup elements vector
   * +- Array Writer
   * .  +- ColumnWriterIndex (for array as a whole)
   * .  +- OffsetVectorWriter (A)
   * .  .  +- Offset vector (B)
   * .  +- ArrayElementWriterIndex
   * .  +- ScalarWriter (D)
   * .  .  +- ArrayElementWriterIndex
   * .  .  +- Scalar vector (D)
   * </code></pre>
   * <p>
   * The top group of objects point into the writer objects in the second
   * group. Letters in parens show the connections.
   * <p>
   * To perform the roll-over, we must:
   * <ul>
   * <li>Copy values from the current vectors to a set of new, look-ahead
   * vectors.</li>
   * <li>Swap buffers between the main and "backup" vectors, effectively
   * moving the "full" batch to the sidelines, putting the look-ahead vectors
   * into play in order to finish writing the current row.</li>
   * <li>Update the writers to point to the look-ahead buffers, including
   * the initial set of data copied into those vectors.</li>
   * <li>Update the vector indexes to point to the next write positions
   * after the values copied during roll-over.</li>
   * </ul>
   *
   * @param cardinality the number of outer elements to create in the look-ahead
   * vector
   */

  @Override
  public void rollover(int cardinality) {

    // Swap out the two vectors. The index presented to the caller
    // is that of the data vector: the next position in the data
    // vector to be set into the data vector writer index.

    valuesState.rollover(childCardinality(cardinality));
    offsetsState.rollover(cardinality);
  }

  @Override
  public void harvestWithLookAhead() {
    offsetsState.harvestWithLookAhead();
    valuesState.harvestWithLookAhead();
  }

  @Override
  public void startBatchWithLookAhead() {
    offsetsState.startBatchWithLookAhead();
    valuesState.startBatchWithLookAhead();
  }

  @Override
  public void reset() {
    offsetsState.reset();
    valuesState.reset();
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("schema", schema)
      .attributeIdentity("writer", arrayWriter)
      .attributeIdentity("vector", vector)
      .attribute("offsetsState");
    offsetsState.dump(format);
    format
      .attribute("valuesState");
    valuesState.dump(format);
    format
      .endObject();
  }
}
